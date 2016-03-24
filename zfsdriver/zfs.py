import socket
from oslo.config import cfg
from cinder.volume import driver
from cinder import exception
from cinder.openstack.common import log as logging
from cinder import utils
from cinder.i18n import _
from cinder.volume import utils as cutils
from cinder.volume.drivers.sursenzfs import zfscmd
from cinder.volume.drivers.sursenzfs import common as zfscm
from cinder.volume.drivers.sursenzfs import zfsclientcall as client_call
from cinder.brick.iscsi import iscsi
from cinder.openstack.common import processutils as putils
from cinder.openstack.common import units
from cinder.brick import exception as bexception
LOG = logging.getLogger(__name__)
volume_opts = [
    cfg.StrOpt('zfspool',
               default='cinder-zfs',
               help='Name for the pool that will contain exported volumes'),
    cfg.IntOpt('zfs_mirrors',
               default=0,
               help='If >0, create LVs with multiple mirrors. Note that '
                    'this requires zfs_mirrors + 2 LVs with available space'),
    cfg.StrOpt('zfs_type',
               default='default',
               help='Type of zfs volumes to deploy; (default or thin)'),
    cfg.IntOpt('num_shell_tries',
               default=3,
               help='Number of times to attempt to run flakey shell commands'),
    cfg.IntOpt('reserved_percentage',
               default=0,
               help='The percentage of backend capacity is reserved'),
    cfg.IntOpt('iscsi_num_targets',
               default=100,
               help='The maximum number of iSCSI target IDs per host'),
    cfg.StrOpt('iscsi_target_prefix',
               default='iqn.2010-10.org.openstack:',
               help='Prefix for iSCSI volumes'),
    cfg.StrOpt('iscsi_ip_address',
               default='$my_ip',
               help='The IP address that the iSCSI daemon is listening on'),
    cfg.IntOpt('iscsi_port',
               default=3260,
               help='The port that the iSCSI daemon is listening on'),
    cfg.IntOpt('num_volume_device_scan_tries',
               deprecated_name='num_iscsi_scan_tries',
               default=3,
               help='The maximum number of times to rescan targets'
                    ' to find volume'),
    cfg.StrOpt('volume_backend_name',
               default=None,
               help='The backend name for a given driver implementation'),
    cfg.BoolOpt('use_multipath_for_image_xfer',
                default=False,
                help='Do we attach/detach volumes in cinder using multipath '
                     'for volume to image and image to volume transfers?'),
    cfg.StrOpt('volume_clear',
               default='zero',
               help='Method used to wipe old volumes (valid options are: '
                    'none, zero, shred)'),
    cfg.IntOpt('volume_clear_size',
               default=0,
               help='Size in MiB to wipe at start of old volumes. 0 => all'),
    cfg.StrOpt('volume_clear_ionice',
               default=None,
               help='The flag to pass to ionice to alter the i/o priority '
                    'of the process used to zero a volume after deletion, '
                    'for example "-c3" for idle only priority.'),
    cfg.StrOpt('iscsi_helper',
               default='tgtadm',
               help='iSCSI target user-land tool to use. tgtadm is default, '
                    'use lioadm for LIO iSCSI support, iseradm for the ISER '
                    'protocol, or fake for testing.'),
    cfg.StrOpt('volumes_dir',
               default='$state_path/volumes',
               help='Volume configuration file storage '
               'directory'),
    cfg.StrOpt('iet_conf',
               default='/etc/iet/ietd.conf',
               help='IET configuration file'),
    cfg.StrOpt('lio_initiator_iqns',
               default='',
               help=('Comma-separated list of initiator IQNs '
                     'allowed to connect to the '
                     'iSCSI target. (From Nova compute nodes.)')),
    cfg.StrOpt('iscsi_iotype',
               default='fileio',
               help=('Sets the behavior of the iSCSI target '
                     'to either perform blockio or fileio '
                     'optionally, auto can be set and Cinder '
                     'will autodetect type of backing device')),
    cfg.StrOpt('volume_dd_blocksize',
               default='1M',
               help='The default block size used when copying/clearing '
                    'volumes'),
    cfg.StrOpt('volume_copy_blkio_cgroup_name',
               default='cinder-volume-copy',
               help='The blkio cgroup name to be used to limit bandwidth '
                    'of volume copy'),
    cfg.IntOpt('volume_copy_bps_limit',
               default=0,
               help='The upper limit of bandwidth of volume copy. '
                    '0 => unlimited'),
    cfg.StrOpt('iscsi_write_cache',
               default='on',
               help='Sets the behavior of the iSCSI target to either '
                    'perform write-back(on) or write-through(off). '
                    'This parameter is valid if iscsi_helper is set '
                    'to tgtadm or iseradm.'),
    cfg.StrOpt('driver_client_cert_key',
               default=None,
               help='The path to the client certificate key for verification, '
                    'if the driver supports it.'),
    cfg.StrOpt('driver_client_cert',
               default=None,
               help='The path to the client certificate for verification, '
                    'if the driver supports it.'),
    cfg.IntOpt('check_times_where_resume',
               default=3,
               help='only for migration when instance is suspended or shut off'),
    cfg.IntOpt('disk_copy_speed',
               default=40,
               help='disk copy speed'),

]

CONF = cfg.CONF
CONF.register_opts(volume_opts)

class TargetIscsiBase(iscsi.LioAdm,driver.ISCSIDriver):
    def __init__(self,execute=utils.execute,*args,**kwargs):
        self._execute=execute
        driver.ISCSIDriver.__init__(self,*args, **kwargs)        
        r_helper=utils.get_root_helper()
        iscsi.LioAdm.__init__(self,root_helper=r_helper, 
                              execute=self._execute)
    
    def set_execute(self,execute):
        self._execute=execute
        
    

class ZFS(driver.ISCSIDriver):
    def __init__(self,root_helper='',physical_volumes=None, zfs_type='default',executr=utils.execute,*args,**kwargs):
        self._execute=executr
        super(ZFS, self).__init__(execute=self._execute, root_helper=root_helper,*args,**kwargs)
        
    def get_zfs_dev_list(self):
        return None
    def get_execute(self):
        return self._execute
        
class ZFSVolumeDriver(driver.VolumeDriver):  
    VERSION = '2.0.0'
    def __init__(self, *args, **kwargs):
        self.db = kwargs.get('db')
        super(ZFSVolumeDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(volume_opts)
        self._execute=utils.execute
        r_helper=utils.get_root_helper()
        self.nova_client_call=client_call.NovaCmdExecute()
        
        self.iscsiobj=ZFS(root_helper=r_helper,*args, **kwargs)
        self.target_helper = self.iscsiobj.get_target_helper(self.db)
        self.targetbase=TargetIscsiBase(*args, **kwargs)
                       
        self.hostname = socket.gethostname()
        self.backend_name =\
               self.configuration.safe_get('volume_backend_name') or 'ZFS_HALF_iSCSI'
        self.poolname=self.configuration.safe_get('volume_pool_name') or ''
        self.volume_dd_bksize=self.configuration.safe_get('volume_dd_blocksize')
        
        if self.volume_dd_bksize.isdigit() is False:
            self.volume_dd_bksize='1M'
        else:
            if int(self.volume_dd_bksize) > 1000 or int(self.volume_dd_bksize) < 1024:
                self.volume_dd_bksize='1M'
            else:
                bksize=int(self.volume_dd_bksize) * 1024
                self.volume_dd_bksize=bksize
        
        self.protocol = 'ZFS_iSCSI'
        self.private_keymgr_init()
        self.cmdcls=zfscmd.ZFSVolumeCMD(pool_name=self.poolname)
        self.configuration.zfspool=self.poolname
        self._stats = {}
        LOG.info('zfs driver_init_ok')
        
    def private_keymgr_init(self):
        ini_cfg=zfscm.INIConfig(path='/etc/cinder/cinder.conf')
        self.poolname=ini_cfg.clean_get('zfs', 'volume_pool_name')
    
    def nova_instance_power_on(self,context,instance_id):
        self.nova_client_call.power_on_instance(context, instance_id)
    
    def nova_instance_shutoff(self,context,instance_id):
        self.nova_client_call.shut_off_instance(context,instance_id)
    
    def nova_instance_suspend(self,context,instance_id):
        self.nova_client_call.suspend_instance(context,instance_id)
    
    def nova_instance_unsuspend(self,context,instance_id):
        self.nova_client_call.unsuspend_instance(context, instance_id)
    
    def nova_get_instance_status(self,context,instance_id):
        return self.nova_client_call.get_instance_status(context, instance_id)
    
    def nova_resume_instance_status(self,context,instance_id,volume_name,volume_size,action_flag):
        vol_dir='/dev/' + self.poolname + '/' + volume_name
        chk_p=self.configuration.safe_get('check_times_where_resume')
        disk_speed=self.configuration.safe_get('disk_copy_speed')
        cmmfunc=zfscm.SYScmdandfun()
        LOG.info('v_size=%s' % volume_size)
        try:
            v_size=int(volume_size)
        except:
            raise NameError('volume size is a wrong value')
        v_s_sign='G'
        fmt_size=cmmfunc.format_size(v_size,v_s_sign,'M')
        s_time=fmt_size/disk_speed
        self.nova_client_call.resume_instance_status(context, instance_id, vol_dir, action_flag, s_time, chk_p)
    
    def set_execute(self, execute):
        self._execute=execute
        return
    
    def _sizestr(self, size_in_g):
        if int(size_in_g) == 0:
            return '100Mb'
        return '%sGb' % size_in_g
    
    def _escape_snapshot(self, snapshot_name):
        # Linux ZFS reserves name that starts with snapshot, so that
        # such volume name can't be created. Mangle it.
        if '@' in snapshot_name:
            raise NameError('wrong snapshot name')
        return snapshot_name

    def extend_volume(self, volume, new_size):        
        self.cmdcls.reset_volume_size(volume['name'], self._sizestr(new_size))
            
    def create_snapshot(self, snapshot):
        """Creates a snapshot."""

        self.cmdcls.create_zfs_snapshot(self._escape_snapshot(snapshot['name']),
                                   snapshot['volume_name'],
                                   self.configuration.zfs_type)

    def delete_snapshot(self, snapshot):
        """Deletes a snapshot."""

        if self.cmdcls.snapshot_is_exist(snapshot['name']):
            self.cmdcls.delete_zfs_snapshot(snapshot['name'], snapshot['volume_name'])
              
        else:
            LOG.warning(_("snapshot: %s not found, "
                          "skipping delete operations") % snapshot['name'])  
            return True 

    def create_cloned_volume(self, volume, src_vref):
        """Creates a clone of the specified volume."""
        mirror_count = 0
        if self.configuration.zfs_mirrors:
            mirror_count = self.configuration.zfs_mirrors
        LOG.info(_('Creating clone of volume: %s') % src_vref['id'])
        volume_name = src_vref['name']
        temp_id = 'tmp-snap-%s' % volume['id']
        
        self.cmdcls.create_zfs_snapshot(self._escape_snapshot(temp_id), 
                                        volume_name,
                                        self.configuration.zfs_type)
        
        if self.cmdcls.check_volume_exist(volume['name']):
            pass
        else:
            try:
                self.cmdcls.create_zfs_volume(volume['name'],
                            self._sizestr(volume['size']),
                            self.configuration.zfs_type,
                                      mirror_count)
            except:
                self.cmdcls.delete_zfs_snapshot(self._escape_snapshot(temp_id), volume_name)
                raise NameError('failed to create volume')
                      
        try:
       
            self.cmdcls.create_vol_from_snap(volume['name'], 
                                         self._sizestr(volume['size']), 
                                         temp_id, 
                                         src_vref['name'], 
                                         src_vref['size'], 
                                         self.volume_dd_bksize)
        except:       
            self.cmdcls.delete_zfs_snapshot(self._escape_snapshot(temp_id), volume_name)
            self.cmdcls.delete_zfs_volume(volume['name'])
            raise NameError('failed to create volume when copying data')
        
        self.cmdcls.delete_zfs_snapshot(self._escape_snapshot(temp_id), volume_name)                     

    def clone_image(self, volume, image_location, image_id, image_meta):
        return None, False                      
          
    def create_volume(self, volume):
        """Creates a logical volume in default pool(cinder-zfs)."""
        mirror_count = 0
        if self.configuration.zfs_mirrors:
            mirror_count = self.configuration.zfs_mirrors
        self.cmdcls.create_zfs_volume(volume['name'],
                            self._sizestr(volume['size']),
                            self.configuration.zfs_type,
                            mirror_count)

    def _getzfs_target_chap_auth(self, context, iscsi_name):
        try: 
            # 'iscsi_name': 'iqn.2010-10.org.openstack:volume-00000001' 
            vol_id = iscsi_name.split(':volume-')[1] 
            volume_info = self.db.volume_get(context, vol_id) 
            # 'provider_auth': 'CHAP user_id password' 
            if volume_info['provider_auth']: 
                return tuple(volume_info['provider_auth'].split(' ', 3)[1:]) 
        except exception.NotFound: 
            LOG.debug('Failed to get CHAP auth from DB for %s', vol_id) 
       
    def create_export(self, context, volume):
        return self._create_export(context, volume)

    def _create_export(self, context, volume):
        """Creates an export for a logical volume.""" 
        if volume['name'] is None:
            return None
#         devmeg=zfscm.DEVList()
#         devname=devmeg.get_devname_by_volumename(volume['name'])   
#         volume_path = "/dev/%s" % devname 
        volume_path="/dev/" + self.poolname + "/" + volume['name']
        
        #data = self.target_helper.create_export(context,
        #                                        volume,
        #                                        volume_path,
        #                                        self.configuration)
        conf=self.configuration
        iscsi_name = "%s%s" % (conf.iscsi_target_prefix,
                               volume['name'])
        max_targets = conf.safe_get('iscsi_num_targets')
        (iscsi_target, lun) = self.target_helper._get_target_and_lun(context,
                                                       volume,
                                                       max_targets)
        try:
            current_chap_auth = self.target_helper._get_target_chap_auth(context,iscsi_name)
        except:
            current_chap_auth = self._getzfs_target_chap_auth(context,iscsi_name)
            pass
                      
        if current_chap_auth:
            (chap_username, chap_password) = current_chap_auth
        else:
            chap_username = cutils.generate_username()
            chap_password = cutils.generate_password()
        chap_auth = self.target_helper._iscsi_authentication('IncomingUser',
                                               chap_username,
                                               chap_password)
        # NOTE(jdg): For TgtAdm case iscsi_name is the ONLY param we need
        # should clean this all up at some point in the future
        
        tid = self.targetbase.create_iscsi_target(iscsi_name,iscsi_target, 0,
                                       volume_path,
                                       chap_auth,
                                       write_cache=conf.iscsi_write_cache)
        data = {}
        data['location'] = self.target_helper._iscsi_location(
            conf.iscsi_ip_address, tid, iscsi_name, conf.iscsi_port, lun)
        data['auth'] = self.target_helper._iscsi_authentication(
            'CHAP', chap_username, chap_password)

        return {
            'provider_location': data['location'],
            'provider_auth': data['auth'],
        }
    
    def delete_volume(self, volume):
        if self.cmdcls.check_volume_exist(volume['name'])==False:
            return True
        self.cmdcls.delete_zfs_volume(volume['name'])
       
    def remove_export(self, context, volume):
        #self.target_helper.remove_export(context, volume)
        try:
            iscsi_target = self.db.volume_get_iscsi_target_num(context,volume['id'])
        except exception.NotFound:
            LOG.info("Skipping remove_export. No iscsi_target, provisioned for volume: %s"%volume['id'])
            return

        self.targetbase.remove_iscsi_target(iscsi_target, 0, volume['id'], volume['name'])
        
    def validate_connector(self, connector):
        self.iscsiobj.validate_connector(connector)
        
    def initialize_connection(self, volume, connector):
        if CONF.iscsi_helper == 'lioadm':
            #self.target_helper.initialize_connection(volume, connector)
            volume_iqn = volume['provider_location'].split(' ')[1]

            (auth_method, auth_user, auth_pass) = \
                     volume['provider_auth'].split(' ', 3)

        # Add initiator iqns to target ACL
            try:
                self._execute('cinder-rtstool', 'add-initiator',
                          volume_iqn,
                          auth_user,
                          auth_pass,
                          connector['initiator'],
                          run_as_root=True)
            except putils.ProcessExecutionError:
                LOG.error(_("Failed to add initiator iqn %s to target") %connector['initiator'])
                raise bexception.ISCSITargetAttachFailed(volume_id=volume['id'])

        iscsi_properties = self.iscsiobj._get_iscsi_properties(volume)
        return {
            'driver_volume_type': 'iscsi',
            'data': iscsi_properties
        }
        
    def ensure_export(self, context, volume):
        
#         devmeg=zfscm.DEVList()
#         devname=devmeg.get_devname_by_volumename(volume['name'])    
        iscsi_name = "%s%s" % (self.configuration.iscsi_target_prefix,
                               volume['name'])
        #volume_path = "/dev/%s" %devname
        volume_path="/dev/" + self.poolname + "/" + volume['name']
        # NOTE(jdg): For TgtAdm case iscsi_name is the ONLY param we need
        # should clean this all up at some point in the future
        model_update = self._ensure_export(
            context, volume,
            iscsi_name,
            volume_path,
            self.configuration.zfspool,
            self.configuration)
        if model_update:
            self.target_helper.db.volume_update(context, volume['id'], model_update)
            
    def _ensure_export(self, context, volume, iscsi_name, volume_path,
                      vg_name, conf, old_name=None):
        try:
            volume_info = self.target_helper.db.volume_get(context, volume['id'])
        except exception.NotFound:
            LOG.info(_("Skipping ensure_export. No iscsi_target "
                       "provision for volume: %s"), volume['id'])
            return

        (auth_method,
         auth_user,
         auth_pass) = volume_info['provider_auth'].split(' ', 3)
        chap_auth = self.target_helper._iscsi_authentication(auth_method,
                                               auth_user,
                                               auth_pass)

        iscsi_target = 1
        
#        self.target_helper.ensure_export(context, volume, iscsi_name, volume_path, vg_name, conf, old_name)

        self.targetbase.create_iscsi_target(iscsi_name, iscsi_target, 0, volume_path,
                                 chap_auth, check_exit_code=False)

    def create_volume_from_snapshot(self, volume, snapshot):
        """Creates a volume from a snapshot."""
        
        if self.cmdcls.check_volume_exist(volume['name']):
            pass
        else:
            self.cmdcls.create_zfs_volume(volume['name'],
                            self._sizestr(volume['size']),
                            self.configuration.zfs_type,
                                      0) 
      
        self.cmdcls.create_vol_from_snap(volume['name'], self._sizestr(volume['size']),
                                                                        snapshot['name'], 
                                                                        snapshot['volume_name'],
                                                                        snapshot['volume_size'],
                                                                        self.volume_dd_bksize)
                         
    def terminate_connection(self, volume, connector, **kwargs):
        pass
    
    def detach_volume(self, context, volume):
        pass
           
    def _update_volume_stats(self):
        data = {}
        # Note(zhiteng): These information are driver/backend specific,
        # each driver may define these values in its own config options
        # or fetch from driver specific configuration file.
        data["volume_backend_name"] = self.backend_name
        data["vendor_name"] = 'Open Source'
        data["driver_version"] = self.VERSION
        data["storage_protocol"] = self.protocol
        data["pools"] = []
        (l_capacity,f_capacity)=self.cmdcls.get_pool_size()
        total_capacity=float(l_capacity)
        free_capacity=float(f_capacity)
        location_info = \
            ('ZFSVolumeDriver:%(hostname)s:%(zfspool)s'
             ':%(zfs_type)s:%(zfs_mirrors)s' %
             {'hostname': self.hostname,
              'zfspool': self.configuration.zfspool,
              'zfs_type': self.configuration.zfs_type,
              'zfs_mirrors': self.configuration.zfs_mirrors})
        # Skip enabled_pools setting, treat the whole backend as one pool
        # XXX FIXME if multipool support is added to LVM driver.
        single_pool = {}
        single_pool.update(dict(
#            pool_name=data["volume_backend_name"],
            pool_name=self.poolname,
            total_capacity_gb=total_capacity,
            free_capacity_gb=free_capacity,
            reserved_percentage=self.configuration.reserved_percentage,
            location_info=location_info,
            QoS_support=False,
        ))
        data["pools"].append(single_pool)
#        LOG.info(str(data))
        return data

    def get_volume_stats(self, refresh=False):
        """Get volume status.

        If 'refresh' is True, run update the stats first.
        """  
        if refresh:
            self._stats=self._update_volume_stats()

        return self._stats
    def check_for_setup_error(self):
        """Verify that requirements are in place to use ZFS driver."""
        setup_status=self.cmdcls.check_setup_err()
        if setup_status==False:
            raise NameError('ZFS filesystem is not exist!!!')
           
        