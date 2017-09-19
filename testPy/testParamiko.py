
import paramiko
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


def main():
    hostname = '192.168.119.134'
    port = 22
    username = 'arcgis'
    password = 'esri123'
    #cmd = "/home/arcgis/arcgis/server/tools/python /home/arcgis/dataManage/serverHandler/InsertGDB_all.py /home/arcgis/customer100005 /home/arcgis/testNew customer /home/arcgis/template.gdb/client 2 1 UID,T1,X,Y"
    cmd='python /home/arcgis/testAdd2.py'
    sshd = ssh_connect(hostname, username, password)
    stdin, stdout, stderr = ssh_exec_cmd(sshd, cmd)
    err_list = stderr.readlines()

    if len(err_list) > 0:
        print 'ERROR:' + err_list[0]
        exit()

    for item in stdout.readlines():
        print item,
    ssh_close(sshd)


if __name__ == "__main__":
    main()
