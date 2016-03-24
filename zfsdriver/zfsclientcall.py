from oslo.config import cfg
from cinder.openstack.common import log as logging
from novaclient import service_catalog
from novaclient.v1_1 import client as nova_client
from novaclient.v1_1.contrib import assisted_volume_snapshots
from cinder.openstack.common import loopingcall
import threading
from time import sleep
import os

nova_opts = [
    cfg.StrOpt('nova_catalog_info',
               default='compute:nova:publicURL',
               help='Match this value when searching for nova in the '
                    'service catalog. Format is: separated values of '
                    'the form: '
                    '<service_type>:<service_name>:<endpoint_type>'),
    cfg.StrOpt('nova_catalog_admin_info',
               default='compute:nova:adminURL',
               help='Same as nova_catalog_info, but for admin endpoint.'),
    cfg.StrOpt('nova_endpoint_template',
               default=None,
               help='Override service catalog lookup with template for nova '
                    'endpoint e.g. http://localhost:8774/v2/%(project_id)s'),
    cfg.StrOpt('nova_endpoint_admin_template',
               default=None,
               help='Same as nova_endpoint_template, but for admin endpoint.'),
    cfg.StrOpt('os_region_name',
               default=None,
               help='Region name of this node'),
    cfg.StrOpt('nova_ca_certificates_file',
               default=None,
               help='Location of ca certificates file to use for nova client '
                    'requests.'),
    cfg.BoolOpt('nova_api_insecure',
                default=False,
                help='Allow to perform insecure SSL requests to nova'),
    cfg.StrOpt('check_instance_status_interval',
                default=3,
                help='the interval for checking instance'),
    cfg.StrOpt('check_instance_status_times',
                default=3,
                help='the interval for checking instance'),
]

CONF = cfg.CONF
CONF.register_opts(nova_opts)

LOG = logging.getLogger(__name__)

def novaclient(context, admin=False):
    # FIXME: the novaclient ServiceCatalog object is mis-named.
    #        It actually contains the entire access blob.
    # Only needed parts of the service catalog are passed in, see
    # nova/context.py.
    compat_catalog = {
        'access': {'serviceCatalog': context.service_catalog or []}
    }
    sc = service_catalog.ServiceCatalog(compat_catalog)

    nova_endpoint_template = CONF.nova_endpoint_template
    nova_catalog_info = CONF.nova_catalog_info

    if admin:
        nova_endpoint_template = CONF.nova_endpoint_admin_template
        nova_catalog_info = CONF.nova_catalog_admin_info

    if nova_endpoint_template:
        url = nova_endpoint_template % context.to_dict()
    else:
        info = nova_catalog_info
        service_type, service_name, endpoint_type = info.split(':')
        # extract the region if set in configuration
        if CONF.os_region_name:
            attr = 'region'
            filter_value = CONF.os_region_name
        else:
            attr = None
            filter_value = None
        url = sc.url_for(attr=attr,
                         filter_value=filter_value,
                         service_type=service_type,
                         service_name=service_name,
                         endpoint_type=endpoint_type)

    LOG.debug('Novaclient connection created using URL: %s' % url)

    extensions = [assisted_volume_snapshots]

    c = nova_client.Client(context.user_id,
                           context.auth_token,
                           context.project_id,
                           auth_url=url,
                           insecure=CONF.nova_api_insecure,
                           cacert=CONF.nova_ca_certificates_file,
                           extensions=extensions)
    # noauth extracts user_id:project_id from auth_token
    c.client.auth_token = context.auth_token or '%s:%s' % (context.user_id,
                                                           context.project_id)
    c.client.management_url = url
    return c


class NovaCmdExecute(object):
    def __init__(self,clienttype='nova'):
        self.client_type=clienttype
        self.cknum=0
        try:
            self.default_times=int(CONF.check_instance_status_times)
        except:
            self.default_times=3
        try:
            self.default_check_itrv=int(CONF.check_instance_status_interval)
        except:
            self.default_check_itrv=3
        self.ctxt=None
    
    def _wait_for_boot(self,context,server_id):
        self.cknum=0   
        def wait_for_boot():
            self.cknum=self.cknum + 1
            if self.cknum > self.default_times:
                raise loopingcall.LoopingCallDone()
            
            istatus=self.get_instance_status(context, server_id)
            if istatus == 'ACTIVE':
                LOG.info('instance start ok')
                raise loopingcall.LoopingCallDone()
            
        timer = loopingcall.FixedIntervalLoopingCall(wait_for_boot)
        timer.start(interval=self.default_check_itrv).wait()            
        if self.cknum > self.default_times:
            raise NameError('Failed to start instance for time out')    
           
    def unsuspend_instance(self,context,server_id):
        novaclient(context).servers.resume(server_id)
        self._wait_for_boot(context, server_id)
        
    def suspend_instance(self,context,server_id):
        novaclient(context).servers.suspend(server_id)
        LOG.info('instance has be suspended')
    
    def shut_off_instance(self,context,server_id):
        novaclient(context).servers.stop(server_id)
        LOG.info('instance shut off')
        
    def power_on_instance(self,context,server_id):
        novaclient(context).servers.start(server_id)
        self._wait_for_boot(context, server_id)

            
    def get_instance_status(self,context,server_id):
        reclient=novaclient(context).servers.get(server_id)      
        if reclient is None:
            return None
        return reclient.status
    
    def do_resume_action(self,context,server_id,action_sign):
        if action_sign=='SUSPEND':
            self.suspend_instance(context, server_id)
        if action_sign=='SHUTOFF':
            self.power_on_instance(context, server_id)
    def _trd_func_for_resume(self,server_id,volume_dir,action_sign,slp_tm,chk_pices):
        stme=slp_tm/chk_pices
        chk_p=chk_pices
        if stme < 2:
            stme=slp_tm
            chk_p=1
            
        for i in range(chk_p):
            sleep(stme)
            if os.path.exists(volume_dir)==False:
                self.do_resume_action(self.ctxt, server_id, action_sign)
                return
        
        ckn=0
        while ckn < 36:
            ckn=ckn + 1
            sleep(10)
            if os.path.exists(volume_dir)==False:
                self.do_resume_action(self.ctxt, server_id, action_sign)
                return
        return        
    
    def resume_instance_status(self,context,server_id,volume_dir,action_sign,slp_time,chk_pices):
        self.ctxt=context
        t=threading.thread(target=self._trd_func_for_resume,args=(server_id,volume_dir,action_sign,slp_time,chk_pices))
        t.setDaemon(False)
        t.start()
        
        
        

                    