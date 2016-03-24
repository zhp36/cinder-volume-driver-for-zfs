#!/usr/bin/python
# -*- coding:utf-8 -*-
#author:sursen.zhp
#INIConfig: read or write ini
#DEVList: monitor the change for os devices
#SYScmdfunc:common command executer
#---------------------
import sys
import os
import ConfigParser
import subprocess
import shlex
import signal
from __builtin__ import str

class INIConfig(object):
    def __init__(self, path=None):
        self.path = path
        self.cf = ConfigParser.ConfigParser()
        if self.path is None:
            ospath=os.path.abspath('..')
            if self.path=='/usr':
                self.path=ospath + '/lib/python2.7/site-packages/cinder/volume/drivers/sursenzfs/surzfs.ini'
            elif 'cinder.conf' in self.path:
                self.path='/etc/cinder/cinder.conf'
            else:
                self.path=ospath + '/cinder/volume/drivers/sursenzfs/surzfs.ini'
        self.cf.read(self.path)
        self.sign=False
    def get(self,field, key):
        result = ""
        try:
            kkey=key.replace('-','_')
            result = self.cf.get(field, kkey)
        except:
            result = ""
        return result
    
    def clean_get(self,field, key):
        result = ""
        try:
            result = self.cf.get(field, key)
        except:
            result = ""
        return result
    
    def set(self, field, key, value):
        try:
            kkey=key.replace('-','_')
            self.cf.set(field, kkey, value)
        except:
            return False
        self.sign=True
        return True
    def remove_key(self,field,key):
        try:
            kkey=key.replace('-','_')
            self.cf.remove_option(field,kkey)
        except:
            return False
        self.sign=True
        return True
    def op_execute(self):
        if self.sign==False:
            return True        
        try:
            self.cf.write(open(self.path,'w'))
        except:
            return False
        return True

class DEVList(object):
    def __init__(self, path=None):
        self.path=path
        if self.path is None:
            self.path='/dev'
    def get_dev_initial(self):
        self.first_dev_list=os.listdir(self.path)
        
    def get_dev_name(self):
        now_dev_list=os.listdir(self.path)
        for n in now_dev_list:
            if n in self.first_dev_list:
                continue
            else:
                return n
            
    def get_all_devs_for_volume(self):
        devarr=[]
        devs=os.listdir(self.path)
        for n in devs:
            if 'zd' in n:
                devarr.append(n)
        return devarr
    
    def get_devname_by_volumename(self,volume_name):
        if volume_name is None:
            return None
        devs=self.get_all_devs_for_volume()
        lnum=len(devs)
        if lnum ==0:
            return None
        sys_cmd=SYScmdandfun()
        cmdstr='find /dev -type l -print0 | xargs --null file | grep -e' + volume_name
        (out_str,r_code,_)=sys_cmd.sys_cmd_exec_str(cmdstr)
        if r_code >0:
            return None
        for n in devs:
            zn='../' + n
            if zn in out_str:
                return n
        return None
                    
    def check_zfs_exist(self):
        fize=os.listdir(self.path)
        for k in fize:
            if k=='zfs':
                return True
        return False
    
class SYScmdandfun(object): 
    def __init__(self, path=None):
        self.outstr=''
        self.rtcode=1
        self.outerr='' 
        
    def _pre_setup(self):
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    
    def sys_cmd_exec_str(self,cmdstr=None):
        if cmdstr is None:
            return (self.outstr,self.rtcode)
        mchild=subprocess.Popen(cmdstr,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
        self.rtcode=mchild.wait()
        (self.outstr,self.outerr)=mchild.communicate()          
        return (self.outstr,self.rtcode,self.outerr)
                         
    def sys_cmd_exec(self,cmdstr=None):
        if cmdstr is None:
            return (self.outstr,self.rtcode)
        xcmd=shlex.split(cmdstr)
        mchild=subprocess.Popen(xcmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=None,preexec_fn=self._pre_setup)
        self.rtcode=mchild.wait()
        (self.outstr,self.outerr)=mchild.communicate()          
        return (self.outstr,self.rtcode,self.outerr)
    
    def _get_next_char(self,sub_str='',all_str=''):
        if sub_str=='' or all_str=='':
            return None
        index=all_str.find(sub_str)
        if index < 0:
            return None
        slen=len(sub_str)
        return all_str[index + slen]
    
    def get_sizesign_from_str(self,f_str):
        if f_str is None:
            return f_str
        if 'G' in f_str:
            return 'G'
        if 'T' in f_str:
            return 'T'
        if 'M' in f_str:
            return 'M'
    
    def format_size(self,floatstr,signstr,basesign):
        if floatstr is None or signstr is None:
            return floatstr
        if basesign=='G':
            if signstr=='M':
                tmpnum=float(floatstr) / 1024
                return ('%.2f'%tmpnum)
            if signstr=='G':
                return floatstr
            if signstr=='T':
                tmpnum=float(floatstr) * 1000
                return ('%.2f'%tmpnum)
            
        if basesign=='M':
            if signstr=='G':
                tmpnum=float(floatstr) * 1000
                return ('%.2f'%tmpnum)
            if signstr=='M':
                return floatstr
            if signstr=='T':
                tmpnum=float(floatstr) * 1000 * 1000
                return ('%.2f'%tmpnum)
            
        if basesign=='T':
            if signstr=='G':
                tmpnum=float(floatstr) / 1024
                return ('%.2f'%tmpnum)
            if signstr=='T':
                return floatstr
            if signstr=='M':
                tmpnum=float(floatstr) / 1024 / 1024
                return ('%.2f'%tmpnum)       
              
        return floatstr        
        
    
    def get_float_from_str(self,cstr=None):
        if cstr is None:
            return None
        def f(x):
            if str(x).isdigit() or str(x)==".":
                return x
            return
        return filter(f,cstr)
            
