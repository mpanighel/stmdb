"""Client to handle connections and actions executed against a remote host."""

from os import system
from paramiko import SSHClient, AutoAddPolicy, RSAKey
from paramiko.auth_handler import AuthenticationException, SSHException
from scp import SCPClient, SCPException

class RemoteClient:
    """Client to interact with a remote host via SSH & SCP."""

    def __init__(self, host, user, ssh_key_path, remote_path):
        self._host = host
        self._user = user
        self._ssh_key_path = ssh_key_path
        self._remote_path = remote_path
        self._ssh_client = None
        self._scp = None
        self._connect()

    def _get_ssh_key(self):
        """
        Fetch locally stored SSH key.
        """
        try:
            self.ssh_key = RSAKey.from_private_key_file(self._ssh_key_path)
            print(f'Found SSH key at self {self._ssh_key_path}')
        except SSHException as error:
            print(error)
        return self.ssh_key

    def _upload_ssh_key(self):
        try:
            system(f'ssh-copy-id -i {self._ssh_key_path} {self._user}@{self._host}>/dev/null 2>&1')
            system(f'ssh-copy-id -i {self._ssh_key_path}.pub {self._user}@{self._host}>/dev/null 2>&1')
            print(f'{self._ssh_key_path} uploaded to {self._host}')
        except FileNotFoundError as error:
            print(error)

    def _connect(self):
        """
        Open connection to remote host.
        """
        try:
            self._ssh_client = SSHClient()
            self._ssh_client.load_system_host_keys()
            self._ssh_client.set_missing_host_key_policy(AutoAddPolicy())
            self._ssh_client.connect(
                hostname=self._host,
                username=self._user,
                key_filename=self._ssh_key_path,
                look_for_keys=True,
                timeout=5000)
            self._scp = SCPClient(self._ssh_client.get_transport())
        except AuthenticationException as error:
            print('Authentication failed: did you remember to create an SSH key?')
            print(error)
            raise error
        return self._ssh_client

    def disconnect(self):
        """
        Close the connection.
        """
        self._scp.close()
        self._ssh_client.close()

    def _bulk_upload(self, files):
        """
        Upload multiple files to a remote directory.

        :param files: List of strings representing file paths to local files.
        """
        uploads = [self._upload_single_file(file) for file in files]
        print(f'Finished uploading {len(uploads)} files to {self._remote_path} on {self._host}')

    def _upload_single_file(self, file):
        """Upload a single file to a remote directory."""
        try:
            self._scp.put(file,
                         recursive=True,
                         remote_path=self._remote_path)
        except SCPException as error:
            print(error)
            raise error
        finally:
            print(f'Uploaded {file} to {self._remote_path}')

    def _download_file(self, file):
        """Download file from remote host."""
        self._scp.get(file)

    def _execute_commands(self, commands):
        """
        Execute multiple commands in succession.

        :param commands: List of unix commands as strings.
        """
        for cmd in commands:
            stdin, stdout, stderr = self._ssh_client.exec_command(cmd)
            stdout.channel.recv_exit_status()
            response = stdout.readlines()
            for line in response:
                #print(f'INPUT: {cmd} | OUTPUT: {line}')
                print(line)
