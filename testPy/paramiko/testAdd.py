import paramiko
from multiprocessing import Pool


def ssh_connect(_host, _username, _password):
    try:
        _ssh_fd = paramiko.SSHClient()
        _ssh_fd.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        _ssh_fd.connect(_host, username=_username, password=_password)
        return _ssh_fd
    except Exception, e:
        print('ssh %s@%s: %s' % (_username, _host, e))
        exit()


def ssh_exec_cmd(_ssh_fd, _cmd):
    return _ssh_fd.exec_command(_cmd)


def ssh_close(_ssh_fd):
    _ssh_fd.close()


def encodeProcess():
    hostname = '192.168.119.134'
    port = 22
    username = 'arcgis'
    password = 'esri123'
    cmd = '/home/arcgis/arcgis/server/tools/python /home/arcgis/dataManage/serverHandler/InsertGDB_large.py /home/arcgis/customer5000000011 /home/arcgis/test customertest /home/arcgis/customerdata.gdb/customerdata 13 14 UID,T1,T7,T30,T90,T180,BANKID1,BANKID2,AGE,SEX,ASSETS_DIS,BANKCARD_D,BRANCHID,X,Y 3'

    sshd = ssh_connect(hostname, username, password)
    stdin, stdout, stderr = ssh_exec_cmd(sshd, cmd)
    err_list = stderr.readlines()

    if len(err_list) > 0:
        print 'ERROR:' + err_list[0]
        exit()

    for item in stdout.readlines():
        print item,
    ssh_close(sshd)


def main():
    pool = Pool(processes=3)
    pool.map(encodeProcess, [2, 2, 2])
    pool.close()
    pool.join()


if __name__ == "__main__":
    try:
        encodeProcess()
    except Exception as e:
        print e.message
    # main()
