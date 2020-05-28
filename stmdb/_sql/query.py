import pickle
from .._ssh import RemoteClient

class SqlQuery(RemoteClient):

    def __init__(self, host, user, ssh_key_path=".ssh/id_rsa"):
        remote_path="/data"
        RemoteClient.__init__(self, host, user, ssh_key_path, remote_path)

    def _sql_query(self, sql):
        sql_encoded = self._sql_encode(sql)
        cmd = "{python} {script} {sql}".format(
            python="virtual_env/bin/python",
            script="db_pickle_script.py",
            sql=sql_encoded)
        self._execute_commands([cmd,])
        self._download_file("results.p")
        output = pickle.load(open("results.p", "rb"))
        return output

    def sql_get(self, key, value, column):
        '''Get metadata from the database.
        
        Given key and value, the column metadata is returned
        according to the SQL query:
        
        SELECT {column} FROM meta WHERE meta.{key}="{value}"
        
        Returns: output of the SQL query.
        '''
        sql = 'SELECT {column} FROM meta WHERE meta.{key}="{value}"'.format(
            column=column,
            key=key,
            value=value)
        return self._sql_query(sql)

    def _sql_set(self, key, value, column, meta):
        sql = 'UPDATE meta SET meta.{column}="{meta}" WHERE {key}="{value}"'.format(
            column=column,
            meta=meta,
            key=key,
            value=value)
        return self._sql_query(sql)

    def get_tags(self, imageID):
        '''Get the tags of the given image.
        
        Arguments:
        imageID: ID of the image as indexed in the database.
        
        Returns: list of tags of the given image.
        '''
        tags = self.sql_get("ID", imageID, "Tags")[0][0]
        if tags:
            return tags.split(",")
        else:
            return list()

    def set_tags(self, imageID, tags):
        '''Replace the tags of the given image.
        
        Arguments:
        imageID: ID of the image as indexed in the database.
        tags: string or list of the new tags.
        
        Returns: new list of tags.
        '''
        if type(tags) is list:
            tags = ",".join(tags)
        self._sql_set("ID", imageID, "Tags", tags)
        return self.get_tags(imageID)

    def append_tags(self, imageID, new_tags):
        '''Append additional tags to the given image.
        
        Arguments:
        imageID: ID of the image as indexed in the database.
        tags: string or list of the additional tags.
        
        Returns: updated list of tags.
        '''
        if type(new_tags) is not list:
            new_tags = new_tags.split(",")
        all_tags = new_tags + self.get_tags(imageID)
        return self.set_tags(imageID, all_tags)

    def get_meta(self, imageID):
        '''Get the metadata of the given image.
        
        Arguments:
        imageID: ID of the image as indexed in the database.
        
        Returns: tuple of metadata for the given image.
        '''
        return self.sql_get("ID", imageID, "*")[0]

    def _set_multiple_tags(self, key, value, tags):
        return self._sql_set(key, value, "Tags", tags)
    
    def _sql_encode(self, sql):
        sql_encoded = str(sql).replace('"', '---')
        sql_encoded = sql_encoded.replace(' ', '__')
        return sql_encoded
