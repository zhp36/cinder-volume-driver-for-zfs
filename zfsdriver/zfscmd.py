
from cinder.i18n import _
from cinder.openstack.common import log as logging
from cinder.openstack.common import processutils as putils
from cinder.volume.drivers.sursenzfs import common as zfscm
from cinder.volume.drivers.sursenzfs import zfsexceptions as zfserr
from cinder.volume import utils as vutils
from cinder import utils
from __builtin__ import True
import os
LOG = logging.getLogger(__name__)

class ZFSVolumeCMD():
    def __init__(self, pool_name='cinder-zfs', zfs_type='default',
                 physical_volumes=None,devdir='/dev',defaultdisk='sdb'):

        """Initialize the zfs object.

        The zfs volume object is based on an zfs pool, one instantiation
        for each zfs pool you have/use.

        :param physical_volumes: List of PVs to build VG on
        :param zfs_type: zfs type (default, or thin)
        :param devdir: os device default dir
        """
        self.poolname=pool_name
        self.dev_dir=devdir
        self.default_disk=defaultdisk
        self.r_helper=utils.get_root_helper()
        self._execute=utils.execute
        self.zfsdlist=zfscm.DEVList(path=devdir)
        self.cmdexec=zfscm.SYScmdandfun()
                    
    def check_setup_err(self):
        if self.zfsdlist.check_zfs_exist()==False:
            return False
        cmdstr=['zpool','list']
        (getpoolstr,_)=self._execute(*cmdstr,root_helper=self.r_helper,run_as_root=True)
        try:
            pass
        except putils.ProcessExecutionError as err:
            LOG.exception(_('Error:zfs file system dose not exist'))
            LOG.error(_('Cmd     :%s') % err.cmd)
            LOG.error(_('StdOut  :%s') % err.stdout)
            LOG.error(_('StdErr  :%s') % err.stderr)
            return False
            
        if not (self.poolname in getpoolstr):
            dev_dir_disk=self.dev_dir + '/' + self.default_disk
            cmdstr=['zpool','create','-f',self.poolname,dev_dir_disk]
            try:
                (getpoolstr,_)=self._execute(*cmdstr,root_helper=self.r_helper,run_as_root=True)
            except putils.ProcessExecutionError as err:
                LOG.exception(_('Error:failed to create root pool'))
                LOG.error(_('Cmd     :%s') % err.cmd)
                LOG.error(_('StdOut  :%s') % err.stdout)
                LOG.error(_('StdErr  :%s') % err.stderr)
                return False          
        return True
    
    def check_volume_exist(self,volumename=None):
        if volumename is None:
            return False
        cmdstr=['zfs','list']
        try:
            (vname,_)=self._execute(*cmdstr,root_helper=self.r_helper,run_as_root=True)
        except putils.ProcessExecutionError as err:
            LOG.exception(_('Error:zfs file system dose not exist'))
            LOG.error(_('Cmd     :%s') % err.cmd)
            LOG.error(_('StdOut  :%s') % err.stdout)
            LOG.error(_('StdErr  :%s') % err.stderr)
            return False
        if volumename in vname:
            return True
        return False
    
    def get_pool_size(self):
        cmdstr=['zpool','get','size',self.poolname]
        pname_len=len(self.poolname)
        try:
            (rtmsg,_)=self._execute(*cmdstr,root_helper=self.r_helper,run_as_root=True)
            r_index=rtmsg.find(self.poolname)
            poolsize=self.cmdexec.get_float_from_str(rtmsg[r_index + pname_len:])
            s_str=self.cmdexec._get_next_char(poolsize, rtmsg)
            poolsize=self.cmdexec.format_size(poolsize, s_str,'G')
        except putils.ProcessExecutionError as err:
            LOG.exception(_('Error:failed to get pool size'))
            LOG.error(_('Cmd     :%s') % err.cmd)
            LOG.error(_('StdOut  :%s') % err.stdout)
            LOG.error(_('StdErr  :%s') % err.stderr)
            poolsize=0.0
            
        cmdstr=['zfs','get','avail',self.poolname]
        try:
            (rtmsg,_)=self._execute(*cmdstr,root_helper=self.r_helper,run_as_root=True)
            r_index=rtmsg.find(self.poolname)
            freesize=self.cmdexec.get_float_from_str(rtmsg[r_index + pname_len:])
            s_str=self.cmdexec._get_next_char(freesize, rtmsg)
            freesize=self.cmdexec.format_size(freesize, s_str,'G')
        except putils.ProcessExecutionError as err:
            LOG.exception(_('Error:failed to get the free size of pool'))
            LOG.error(_('Cmd     :%s') % err.cmd)
            LOG.error(_('StdOut  :%s') % err.stdout)
            LOG.error(_('StdErr  :%s') % err.stderr)
            freesize=0.0
            
        return (poolsize,freesize)
    
    def delete_zfs_volume(self,name):
        if name is None:
            return
        kname=self.poolname + '/' + name 
        cmdstr=['zfs','destroy','-R',kname]
        try:
            (rtmsg,_)=self._execute(*cmdstr,root_helper=self.r_helper,run_as_root=True)           
        except putils.ProcessExecutionError as err:
            LOG.error(_('Cmd     :%s') % err.cmd)
            LOG.error(_('StdOut  :%s') % err.stdout)
            LOG.error(_('StdErr  :%s') % err.stderr)
            raise NameError('Error:failed to delete zfs volume:%s' % name)
        
        if 'cannot' in rtmsg and 'busy' in rtmsg:
            raise NameError(rtmsg)
        
    def set_property_of_volume(self,property_name,new_size,p_name,raise_sign=True):
        v_sizestr=property_name + '=' + new_size
       # v_sizestr='reservation=' + new_size
        cmdstr=['zfs','set',v_sizestr,p_name]
        try:
            self._execute(*cmdstr,root_helper=self.r_helper,run_as_root=True)           
        except putils.ProcessExecutionError as err:
            if raise_sign is True:
                LOG.error(_('Cmd     :%s') % err.cmd)
                LOG.error(_('StdOut  :%s') % err.stdout)
                LOG.error(_('StdErr  :%s') % err.stderr)
                raise NameError('Error:failed to set %s of volume:%s' % (property_name,p_name))
            else:
                pass                                 
           
    def create_zfs_volume(self, name, size_str, zfs_type='default', mirror_count=0):
        """Creates a logical volume in default pool(cinder-zfs)

        :param name: Name to use when creating Logical Volume
        :param size_str: Size to use when creating Logical Volume
        :param zfs_type: Type of Volume (default or thin)
        :param mirror_count: Use zfs mirroring with specified count

        """
        if name is None or size_str is None:
            LOG.error(_('Failed to create volume:%s,name or size can not be None')%name)
            return
        pname=self.poolname + '/' + name
        cmdstr=['zfs','create','-V',size_str,pname]
        self.zfsdlist.get_dev_initial()
        try:
            self._execute(*cmdstr,root_helper=self.r_helper,run_as_root=True)           
        except putils.ProcessExecutionError as err:
            LOG.error(_('Cmd     :%s') % err.cmd)
            LOG.error(_('StdOut  :%s') % err.stdout)
            LOG.error(_('StdErr  :%s') % err.stderr)
            raise NameError('Error:failed to create zfs volume:%s' % name)        
        
        newdev=self.zfsdlist.get_dev_name()
        if newdev is None:
            raise NameError('Device for volume:%s create failure!!!' % name)
        
        self.set_property_of_volume('reservation',size_str, pname, raise_sign=False)
                            
    def snapshot_is_exist(self,snapshot_name):
        
        cmdstr=['zfs','list','-t','snapshot']        
        try:
            (listname,_)=self._execute(*cmdstr,root_helper=self.r_helper,run_as_root=True)           
        except putils.ProcessExecutionError as err:
            LOG.error(_('Cmd     :%s') % err.cmd)
            LOG.error(_('StdOut  :%s') % err.stdout)
            LOG.error(_('StdErr  :%s') % err.stderr)
            raise NameError('Error:failed to list snapshots')        
                
        if snapshot_name in listname:
            return True
        
        return False
        
    def create_zfs_snapshot(self, name, source_zv_name, zfs_type='default'):
        """Creates a snapshot of a logical volume.

        :param name: Name to assign to new snapshot
        :param source_zv_name: Name of Logical Volume to snapshot
        :param zfs_type: Type of ZV (now is nothing)

        """
        pname=self.poolname + '/' + source_zv_name + '@' + name
        cmdstr=['zfs','snapshot',pname]

        try:
            self._execute(*cmdstr,root_helper=self.r_helper,run_as_root=True)           
        except putils.ProcessExecutionError as err:
            LOG.error(_('Cmd     :%s') % err.cmd)
            LOG.error(_('StdOut  :%s') % err.stdout)
            LOG.error(_('StdErr  :%s') % err.stderr)
            raise NameError('Error:failed to create snapshot for zfs volume:%s' % source_zv_name)

    def delete_zfs_snapshot(self,snapshot_name,volume_name):
        
        pname=self.poolname + '/' + volume_name + '@' + snapshot_name
        cmdstr=['zfs','destroy',pname]
        
        try:
            (rtmsg,_)=self._execute(*cmdstr,root_helper=self.r_helper,run_as_root=True)           
        except putils.ProcessExecutionError as err:
            LOG.error(_('Cmd     :%s') % err.cmd)
            LOG.error(_('StdOut  :%s') % err.stdout)
            LOG.error(_('StdErr  :%s') % err.stderr)
            raise NameError('Error:failed to delete snapshot:%s' % pname)
        
        if 'cannot' in rtmsg and 'busy' in rtmsg:
            raise NameError(rtmsg)
        
    def _clone_snap_to_volume(self,src_snap_name,des_volume_name):
        
        if self.check_volume_exist(des_volume_name):
            return 
        if not self.snapshot_is_exist(src_snap_name):
            raise NameError('The snapshot :%s is not exist '% src_snap_name)
               
        cmdstr=['zfs','clone',src_snap_name,des_volume_name]        
        try:
            self._execute(*cmdstr,root_helper=self.r_helper,run_as_root=True)           
        except putils.ProcessExecutionError as err:
            LOG.exception(_('Error:failed to clone snapshot:%s')%src_snap_name)
            LOG.error(_('Cmd     :%s') % err.cmd)
            LOG.error(_('StdOut  :%s') % err.stdout)
            LOG.error(_('StdErr  :%s') % err.stderr)
            raise NameError('CMD error')
        if self.check_volume_exist(des_volume_name):
            pass
        else:
            raise NameError('failed to create volume:%s'% des_volume_name)

    def _copy_volume(self,src_vol_dir,des_vol_dir,src_vol_size,vol_dd_size,sync=False):
        if src_vol_dir is None or des_vol_dir is None:
            return False

        bksize,count=vutils._calculate_count(src_vol_size, vol_dd_size)

        
        extra_flags = []
        if vutils.check_for_odirect_support(src_vol_dir, des_vol_dir, 'iflag=direct'):
            extra_flags.append('iflag=direct')

        if vutils.check_for_odirect_support(src_vol_dir, des_vol_dir, 'oflag=direct'):
            extra_flags.append('oflag=direct') 
        
        if sync and not extra_flags:
            extra_flags.append('conv=sync')
        
        cmdstr = ['dd', 'if=%s' % src_vol_dir, 'of=%s' % des_vol_dir,
           'count=%d' % count, 'bs=%s' % bksize]
        cmdstr.extend(extra_flags)    

         
        try:
            self._execute(*cmdstr,root_helper=self.r_helper,run_as_root=True)          
        except putils.ProcessExecutionError as err:
            LOG.error(_('Cmd     :%s') % err.cmd)
            LOG.error(_('StdOut  :%s') % err.stdout)
            LOG.error(_('StdErr  :%s') % err.stderr)
            raise NameError('Error:failed to dd data from snapshot:%s' % src_vol_dir)        
        
        return True
        
    
    def create_vol_from_snap(self,vol_name,vol_size,snap_name,snap_volume_name,snap_vol_size,dd_size):
        
#         xstr='sudo cinder-rootwrap /etc/cinder/rootwrap.conf zfs send zfs-pool2/zvl002aa@a | zfs recv zfs-pool2/zvbk'
#         #xstr='sudo -S  zfs send zfs-pool2/zvl002aa@a > /home/vol_test2244'

#         xstr='sudo cinder-rootwrap /etc/cinder/rootwrap zfs list'
#         ks=zfscm.SYScmdandfun()
#         (rtstr,rtn,rterr)=ks.sys_cmd_exec(xstr)
#         fwr=open('/home/zbc','w')
#         fwr.write(rtstr + '-------' + rterr)
#         fwr.close()
#         LOG.info(os.getenv('USER') or os.getenv('USERNAME') or os.getenv('LOGNAME'))
#         return

        d_name=self.poolname + '/' + vol_name
        temp_name=self.poolname + '/' + vol_name + '-tmp'
        s_name=self.poolname + '/' + snap_volume_name + '@' + snap_name
        
        try:
            self._clone_snap_to_volume(s_name, temp_name)
        except:
            self.delete_zfs_volume(vol_name + '-tmp')
            raise NameError('failed to clone volume from snap')
        
        if self.check_volume_exist(temp_name):
            pass
        else:
            raise NameError('failed to create temp volume')
        
        srcvoldir='/dev/' + temp_name
        desvoldir='/dev/' + d_name
        if dd_size=='1M':
            kk_size=1024 * 1024
        else:
            kk_size=dd_size
        
            
        try:
               
            if self._copy_volume(srcvoldir, desvoldir, snap_vol_size * 1024, str(kk_size)):
                self.delete_zfs_volume(vol_name + '-tmp')
            else:
                raise NameError('ERROR:dd copy error')                          
        except:
            self.delete_zfs_volume(vol_name + '-tmp')            
            raise NameError('failed to create volume , rollback ok')
    
    def reset_volume_size(self,vol_name,new_size):
        
        pname=self.poolname + '/' + vol_name
        v_sizestr='volsize=' + new_size
        
        cmdstr=['zfs','set',v_sizestr,pname]
        try:
            self._execute(*cmdstr,root_helper=self.r_helper,run_as_root=True)           
        except putils.ProcessExecutionError as err:
#            LOG.exception(_('Error1:failed to extend the size of volume:%s')%pname)
            LOG.error(_('Cmd     :%s') % err.cmd)
            LOG.error(_('StdOut  :%s') % err.stdout)
            LOG.error(_('StdErr  :%s') % err.stderr)
            raise NameError('Error1:failed to extend the size of volume:%s' % pname)
        size_str=new_size
        self.set_property_of_volume(size_str, pname, raise_sign=False) 
                        
    
    def get_volume_list(self,pool_name=''):
        vl_list=[]
        return vl_list
           
        