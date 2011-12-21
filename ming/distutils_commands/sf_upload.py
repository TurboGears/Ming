import os
import string
from getpass import getpass

from setuptools import Command

class sf_upload(Command):
    description = "Upload a release to SourceForge"
    user_options = [
        ('sf-user=', 'u',
         'SourceForge username'),
        ('sf-project=', 'p',
         'SourceForge project shortname'),
        ('sf-prikey=', 'k',
         'SourceForge private key filename'),
        ]

    def initialize_options(self):
        self.sf_user = None
        self.sf_project = None
        self.sf_prikey = None

    def finalize_options(self):
        pass

    def run(self):
        import paramiko
        ssh = paramiko.SSHClient()
        host = 'frs.sourceforge.net'
        username='%s,%s' % (self.sf_user, self.sf_project)
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        pkey = None
        if self.sf_prikey:
            for ktype in paramiko.RSAKey, paramiko.DSSKey:
                try:
                    pkey = ktype.from_private_key_file(self.sf_prikey)
                except paramiko.PasswordRequiredException:
                    pkey = ktype.from_private_key_file(
                        self.sf_prikey, getpass('Password for %s: ' % self.sf_prikey))
                except paramiko.SSHException:
                    pass
        if pkey:
            ssh.connect(
                host,
                username=username,
                pkey=pkey,
                look_for_keys=False,
                allow_agent=False)
        else:
            ssh.connect(
                host,
                username=username,
                password=getpass('Password:'),
                pkey=pkey,
                look_for_keys=False,
                allow_agent=False)
            
        sftp = ssh.open_sftp()
        shortname = self.sf_project
        release = self.distribution.get_version()
        download_url_tpl = string.Template(
            'http://downloads.sourceforge.net/project/$shortname/$release/$basename')
        sftp.chdir('/home/frs/project/%s/%s/%s' % (
                shortname[0], shortname[:2], shortname))
        if release not in sftp.listdir():
            sftp.mkdir(release)
        for cmd, _, filename in self.distribution.dist_files:
            basename = os.path.basename(filename)
            sftp.put(filename, '%s/%s' % (release, basename))
            self.distribution.metadata.download_url = download_url_tpl.substitute(locals())

