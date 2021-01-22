# -*- coding: utf-8 -*-
#
# Copyright (c) 2018 Microsoft Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

import os
import tempfile
import logging

try:
    from urlparse import urlsplit
except ImportError:
    from urllib.parse import urlsplit

from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError, ResourceExistsError

from luigi.azure_credential import azure_credential
from luigi.format import get_default_format
from luigi.target import FileAlreadyExists, FileSystem, AtomicLocalFile, FileSystemTarget
from luigi.task import ExternalTask
from luigi.parameter import Parameter
from luigi.contrib.utils import _DeleteOnCloseFile

logger = logging.getLogger('luigi-interface')

BLOB_DOMAIN = "blob.core.windows.net"


def parse_azure_path(path):
    """
    Returns account name, container name and blob path from a fully qualified Azure path
    >>> parse_azure_path(wasbs://container@account.blob.core.windows.net/path/to/object)
    "account", "container", "path/to/object"
    """
    (_, netloc, path, _, _) = urlsplit(path)
    path_without_initial_slash = path[1:]

    if "@" not in netloc:
        raise RuntimeError("Incorrect path format, missing container or account name: {}".format(path))
    bucket, account_url = netloc.split("@")
    # Parse the part after the @: it should be "account.<BLOB_DOMAIN>"
    try:
        (account_name, domain) = account_url.split(".", 1)
    except ValueError:
        raise RuntimeError("Incorrect path format: missing {} domain name: {}".format(BLOB_DOMAIN, path))

    if domain != BLOB_DOMAIN:
        raise RuntimeError("Invalid blob domain name: {}. It should be {}".format(domain, BLOB_DOMAIN))
    return account_name, bucket, path_without_initial_slash


class AzureBlobClient(FileSystem):
    """
    Create an Azure Blob Storage client for authentication.
    Users can create multiple storage accounts, each of which acts like a silo. Under each storage account, we can
    create a container. Inside each container, the user can create multiple blobs.

    For each account, there should be an account key. This account key cannot be changed and one can access all the
    containers and blobs under this account using the account key.

    Usually using an account key might not always be the best idea as the key can be leaked and cannot be revoked. The
    solution to this issue is to create Shared `Access Signatures` aka `sas`. A SAS can be created for an entire
    container or just a single blob. SAS can be revoked.
    """
    SCHEME = "wasbs"

    def __init__(self, account_name=None, account_key=None, sas_token=None, **kwargs):
        """
        :param str account_name:
            The storage account name. This is used to authenticate requests signed with an account key\
            and to construct the storage endpoint. It is required unless a connection string is given,\
            or if a custom domain is used with anonymous authentication.
        :param str account_key:
            The storage account key. This is used for shared key authentication.
        :param str sas_token:
            A shared access signature token to use to authenticate requests instead of the account key.
        :param dict kwargs:
            A key-value pair to provide additional connection options.

            * `protocol` - The protocol to use for requests. Defaults to https.
            * `connection_string` - If specified, this will override all other parameters besides request session.\
                See http://azure.microsoft.com/en-us/documentation/articles/storage-configure-connection-string/ for the connection string format
            * `endpoint_suffix` - The host base component of the url, minus the account name. Defaults to Azure\
                (core.windows.net). Override this to use the China cloud (core.chinacloudapi.cn).
            * `custom_domain` - The custom domain to use. This can be set in the Azure Portal. For example, ‘www.mydomain.com’.
            * `token_credential` - A token credential used to authenticate HTTPS requests. The token value should be updated before its expiration.
        """
        self.options = {"account_name": account_name, "account_key": account_key, "sas_token": sas_token}
        self.kwargs = kwargs
        self._blob_service_client = None

    @property
    def blob_service_client(self):
        if self._blob_service_client:
            return self._blob_service_client

        if not self.options.get("account_key"):
            # credentials are brought by Emissary: should intercept the request and add credentials
            credential = azure_credential
        else:
            credential = self.options.get("account_key")
        self._blob_service_client = BlobServiceClient(
            account_url="{protocol}://{account_name}.{blob_domain}".format(
                protocol=self.kwargs.get("protocol", "https"),
                account_name=self.options.get("account_name"),
                blob_domain=BLOB_DOMAIN),
            credential=credential,
        )
        return self._blob_service_client

    def _add_path_delimiter(self, key):
        return key if key[-1:] == '/' else key + '/'

    def is_hadoop_folder(self, blob):
        # exclude blobs marked as folders by Hadoop
        # Hadoop has a very specific way of marking directories:
        # it creates a blob, then marks it as a directory with a custom meta
        if hasattr(blob, "metadata") and blob.metadata and blob.metadata.get("hdi_isfolder", "").lower() == "true":
            return True
        return False

    def upload(self, tmp_path, container, blob, metadata=None):
        logging.debug("Uploading file '{tmp_path}' to container '{container}' and blob '{blob}'".format(
            tmp_path=tmp_path, container=container, blob=blob))
        self.create_container(container)

        with open(tmp_path, 'rb') as data:
            self.blob_service_client.get_blob_client(container, blob).upload_blob(data, overwrite=True, metadata=metadata)

    def upload_string(self, data, container, blob, metadata=None):
        logging.debug("Uploading a string to container '{container}' and blob '{blob}'".format(
            container=container, blob=blob))
        self.create_container(container)

        self.blob_service_client.get_blob_client(container, blob).upload_blob(data, overwrite=True, metadata=metadata)

    def upload_file(self, local_path, destination_path, metadata=None):
        logging.debug("Uploading file '{local_path}' to destination '{destination_path}'".format(local_path=local_path,
                                                                                                 destination_path=destination_path))
        (container, blob) = self.splitfilepath(destination_path)
        self.upload(local_path, container, blob, metadata=None)

    def download_as_bytes(self, container, blob, bytes_to_read=None):
        logging.debug("Downloading from container '{container}' and blob '{blob}' as bytes".format(
            container=container, blob=blob))
        offset = 0
        if bytes_to_read is None:
            # workaround Azure client raising Invalid Range when downloading an empty file with offset=0
            # no need to define offset if we download the whole file
            offset = None
        return self.blob_service_client.get_blob_client(container, blob).download_blob(offset=offset, length=bytes_to_read).readall()

    def download_as_file(self, container, blob, location):
        logging.debug("Downloading from container '{container}' and blob '{blob}' to {location}".format(
            container=container, blob=blob, location=location))
        with open(location, 'wb') as f:
            return self.blob_service_client.get_blob_client(container, blob).download_blob().readinto(f)

    def download(self, container, blob):
        """Downloads the object contents to local file system.
        Implementation copied from gcs.py download
        """
        with tempfile.NamedTemporaryFile(delete=False) as fp:
            # We can't return the tempfile reference because of a bug in python: http://bugs.python.org/issue18879
            return_fp = _DeleteOnCloseFile(fp.name, 'r')
            self.download_as_file(container, blob, fp.name)
            return return_fp

    def create_container(self, container_name):
        try:
            return self.blob_service_client.create_container(container_name)
        except ResourceExistsError:
            return self.blob_service_client.get_container_client(container_name)

    def delete_container(self, container_name):
        self.blob_service_client.delete_container(container_name)

    def exists(self, path):
        container, blob = self.splitfilepath(path)

        # If blob was not specified, check if container exists
        if not blob:
            try:
                self.blob_service_client.get_container_client(container).get_container_properties()
            except ResourceNotFoundError:
                return False
            return True

        # directories don't exist by themselves, they only exist if there are blobs inside
        # we can't know if a path is a directory anyway (we tolerate omission of the ending "/")
        # so the best method is listing objects inside: we stop at the first item found
        # if it was a blob and not a directory, list_objects() will return this blob
        # We also make sure not to exclude Hadoop folders since we want to be able to test their existence
        objects = self.list_objects(path, exclude_hadoop_folders=False)
        return any(objects)  # at least one element exists inside the directory

    def get_properties(self, path):
        (container, key) = self.splitfilepath(path)
        blob = self.blob_service_client.get_blob_client(container, key)
        return blob.get_blob_properties()

    def _full_url(self, container, key):
        """
        return a fully qualified URL from a container name and a path

        >>> _full_url("container", "my/file")
        "wasbs://container@account.blob.core.windows.net/my/file"
        """
        full_path = "{}://{}@{}.{}/{}".format(self.SCHEME, container, self.options["account_name"], BLOB_DOMAIN, key)
        return full_path

    def list_objects(self, path, exclude_hadoop_folders=True):
        """
        :rtype: collections.Iterable[dict]
        """
        (container, key) = self.splitfilepath(path)
        dir_with_trailing_slash = self._add_path_delimiter(key)

        blob_service_client = self.blob_service_client
        container_client = blob_service_client.get_container_client(container)
        # list_blobs() default to a prefix search, but we want a strict list of directories
        for blob in container_client.list_blobs(key, include=["metadata"]):
            # exclude blobs marked as folders by Hadoop
            # Hadoop has a very specific way of marking directories:
            # it creates a blob, then marks it as a directory with a custom meta
            if exclude_hadoop_folders and self.is_hadoop_folder(blob):
                continue
            name = blob["name"]
            # allowed cases for the blob returned:
            # - it's exactly the key,
            # - it's a file inside that directory
            # - we're listing the root (key=""), so everything can be returned
            # if you search for "foo", we want only "foo/bar", and not "foobar"
            if name == key or key == "":
                yield blob
            elif name.startswith(dir_with_trailing_slash):
                yield blob

    def list_keys(self, path, exclude_hadoop_folders=True):
        for it in self.list_objects(path, exclude_hadoop_folders):
            yield self._full_url(it["container"], it["name"])

    def remove(self, path, recursive=True, skip_trash=True):

        def safe_delete_blob(container_name, blob):
            self.blob_service_client.get_blob_client(container_name, blob).delete_blob()
        if recursive:
            # We want to make sure to list the path without "/" so as to include the Hadoop folder marker in the returned results
            for key in self.list_keys(path.rstrip("/"), exclude_hadoop_folders=False):
                container, blob = self.splitfilepath(key)
                safe_delete_blob(container, blob)
        else:
            if not self.exists(path):
                return False
            container, blob = self.splitfilepath(path)
            safe_delete_blob(container, blob)
        return True

    def mkdir(self, path, parents=True, raise_if_exists=False):
        path = self._add_path_delimiter(path)
        container, blob = self.splitfilepath(path)
        if raise_if_exists and self.exists(path):
            raise FileAlreadyExists("The Azure blob path '{blob}' already exists under container '{container}'"
                                    .format(blob=blob, container=container))
        self.upload_string("", container, blob)

    def isdir(self, path):
        """
        Azure Blob Storage has no concept of directories. We iterate through blobs starting with the path prefix
        and consider it's a directory if at least one blob starting with this prefix exists.
        :param str path: Path of the Azure blob storage
        :return: True if the path contains other blobs
        """
        path = self._add_path_delimiter(path)
        keys = self.list_keys(path)
        return any(keys)

    def move(self, path, dest):
        try:
            self.copy(path, dest)
            self.remove(path)
        except IOError:
            self.remove(dest)
            return False

    def copy(self, path, dest):
        source_account, _, _ = parse_azure_path(path)
        dest_account, _, _ = parse_azure_path(dest)
        if source_account != dest_account:
            raise Exception(
                "Can't copy blob from '{source_account}' to '{dest_account}'. Files can only be moved within "
                "containers in the same storage account".format(
                    source_account=source_account, dest_account=dest_account
                ))
        source_container, source_blob = self.splitfilepath(path)
        dest_container, dest_blob = self.splitfilepath(dest)

        def copy_blob(source_container, source_blob, dest_container, dest_blob):
            dest_url = self._full_url(dest_container, dest_blob)

            source_blob = self.blob_service_client.get_blob_client(source_container, source_blob)

            self.blob_service_client.get_blob_client(dest_container, dest_blob).start_copy_from_url(
                source_url=source_blob.url,
            )

        # Same implementation as GCS client copy:
        # if the source is a dir, copy each element and assume dest is also a dir
        # copy everything, including hadoop folder markers. the blob metadata will be copied
        if self.isdir(path):
            source_path = self._add_path_delimiter(path)
            src_prefix = self._add_path_delimiter(source_blob)
            dest_prefix = self._add_path_delimiter(dest_blob)

            blobs_in_path = self.list_keys(source_path, exclude_hadoop_folders=False)
            for obj in blobs_in_path:
                suffix = obj[len(source_path):]
                if not suffix:
                    # blob with the same name as the directory: it's likely a hadoop folder marker
                    # don't copy it: it's not part of the directory
                    # Note: listing files with path/ (+ path delimiter) would avoid this but it requires calling
                    # list_keys a second time. At the time we call list_keys we don't know yet if it's a directory.
                    continue
                copy_blob(source_container, src_prefix + suffix, dest_container, dest_prefix + suffix)
        else:
            copy_blob(source_container, source_blob, dest_container, dest_blob)

    def rename_dont_move(self, path, dest):
        self.move(path, dest)

    def _reload_blob_service_client(self, account_name=None):
        # invalidate blob service client property if the account name changes
        # it will be recreated with the right account name
        if account_name != self.options["account_name"]:
            self.options["account_name"] = account_name
            self._blob_service_client = None

    def splitfilepath(self, path):
        account_name, container, path_without_initial_slash = parse_azure_path(path)
        self._reload_blob_service_client(account_name=account_name)
        return container, path_without_initial_slash


class AtomicAzureBlobFile(AtomicLocalFile):
    def __init__(self, container, blob, client, **kwargs):
        super(AtomicAzureBlobFile, self).__init__(os.path.join(container, blob))
        self.container = container
        self.blob = blob
        self.client = client
        self.azure_blob_options = kwargs

    def move_to_final_destination(self):
        self.client.upload(self.tmp_path, self.container, self.blob)


class AzureBlobTarget(FileSystemTarget):
    """
    Create an Azure Blob Target for storing data on Azure Blob Storage
    """
    def __init__(self, path, client=None, format=None, download_when_reading=True, **kwargs):
        """
        :param str path:
            Azure full blob path in the following format: wasbs://container@account.blob.core.windows.net/path/to/object
        :param AzureBlobClient client:
            An instance of :class:`.AzureBlobClient`. If none is specified, DefaultAzureCredential will be used
        :param str format:
            An instance of :class:`luigi.format`.
        :param bool download_when_reading:
            Determines whether the file has to be downloaded to temporary location on disk. Defaults to `True`.

        """
        account_name, container, blob = parse_azure_path(path)
        super(AzureBlobTarget, self).__init__(path)
        if format is None:
            format = get_default_format()
        self.container = container
        self.blob = blob
        self.account_name = account_name
        self.client = client or AzureBlobClient(account_name=self.account_name)
        self.format = format
        self.download_when_reading = download_when_reading
        self.azure_blob_options = kwargs

    @property
    def fs(self):
        """
        The :py:class:`FileSystem` associated with :class:`.AzureBlobTarget`
        """
        return self.client

    def open(self, mode='r'):
        """
        Open the target for reading or writing

        :param char mode:
            'r' for reading and 'w' for writing.

            'b' is not supported and will be stripped if used. For binary mode, use `format`
        :return:
            * :class:`.io.FileIO` if 'r'
            * :class:`.AtomicAzureBlobFile` if 'w'
        """
        if mode not in ('r', 'w'):
            raise ValueError("Unsupported open mode '%s'" % mode)
        if mode == 'r':
            return self.format.pipe_reader(self.client.download(self.container, self.blob))
        else:
            return self.format.pipe_writer(AtomicAzureBlobFile(self.container, self.blob, self.client, **self.azure_blob_options))


class AzureBlobFlagTarget(AzureBlobTarget):
    """
    Equivalent to GCSFlagTarget / S3FlagTarget for Azure Blob Storage
    """
    fs = None

    def __init__(self, path, format=None, client=None, flag='_SUCCESS'):
        """
        Initializes an AzureFlagTarget.

        :param path: the directory where the files are stored.
        :type path: str
        :param client:
        :type client:
        :param flag:
        :type flag: str
        """
        if format is None:
            format = get_default_format()

        if path[-1] != "/":
            raise ValueError("AzureFlagTarget requires the path to be to a "
                             "directory.  It must end with a slash ( / ).")
        super(AzureBlobFlagTarget, self).__init__(path, format=format, client=client)
        self.format = format
        self.fs = client or AzureBlobClient(account_name=self.account_name)
        self.flag = flag

    def exists(self):
        flag_target = self.path + self.flag
        return self.fs.exists(flag_target)


class AzureBlobPathTask(ExternalTask):
    path = Parameter()  # type: str
    task_priority = Parameter(default=0)  # type: int

    # This property allows us to have a different priority value for each instance of this job
    # See https://luigi.readthedocs.io/en/stable/tasks.html?highlight=order#task-priority
    @property
    def priority(self):
        return self.task_priority

    def output(self):
        return AzureBlobTarget(self.path)


class AzureBlobFlagTask(ExternalTask):
    path = Parameter()  # type: str
    flag = Parameter()  # type: str

    def output(self):
        return AzureBlobFlagTarget(self.path, flag=self.flag)
