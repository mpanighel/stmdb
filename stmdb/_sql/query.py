import pickle
import os
from .._ssh import RemoteClient

class SqlQuery(RemoteClient):

    def __init__(self, host, user, ssh_key_path):
        self._db_output = "db_output.p"
        remote_path="/data"
        RemoteClient.__init__(self, host, user, ssh_key_path, remote_path)

    def disconnect(self):
        """
        Close the connection to the database.
        """
        RemoteClient.disconnect(self)
        os.remove(self._db_output)

    def _sql_query(self, sql):
        sql_encoded = self._sql_encode(sql)
        cmd = "{python} {script} {sql}".format(
            python="virtual_env/bin/python",
            script="db_pickle_script.py",
            sql=sql_encoded)
        self._execute_commands([cmd,])
        self._download_file(self._db_output)
        output = pickle.load(open(self._db_output, "rb"))
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
        
        Returns: dictionary of metadata for the given image.
        '''
        values = self.sql_get("ID", imageID, "*")[0]
        keys = ['ID',
                'ImageOriginalName',
                'ImageHashName',
                'Format',
                'Version',
                'System',
                'Date',
                'User',
                'Comment',
                'Name',
                'VGAPContact',
                'FieldXSizeinnm',
                'FieldYSizeinnm',
                'ImageSizeinX',
                'ImageSizeinY',
                'IncrementX',
                'IncrementY',
                'ScanAngle',
                'XOffset',
                'YOffset',
                'TF0_TopographicChannel',
                'TF0_Direction',
                'TF0_MinimumRawValue',
                'TF0_MaximumRawValue',
                'TF0_MinimumPhysValue',
                'TF0_MaximumPhysValue',
                'TF0_Resolution',
                'TF0_PhysicalUnit',
                'TF0_Filename',
                'TF0_DisplayName',
                'TB0_TopographicChannel',
                'TB0_Direction',
                'TB0_MinimumRawValue',
                'TB0_MaximumRawValue',
                'TB0_MinimumPhysValue',
                'TB0_MaximumPhysValue',
                'TB0_Resolution',
                'TB0_PhysicalUnit',
                'TB0_Filename',
                'TB0_DisplayName',
                'SPMMethod',
                'Dualmode',
                'GapVoltage',
                'FeedbackSet',
                'LoopGain',
                'XResolution',
                'YResolution',
                'ScanSpeed',
                'XDrift',
                'YDrift',
                'ScanMode',
                'TopographyTimeperPoint',
                'SpectroscopyGridValueinX',
                'SpectroscopyGridValueinY',
                'SpectroscopyPointsinX',
                'SpectroscopyLinesinY',
                'ZSpeed',
                'ZOutputGain',
                'AutomaticZzero',
                'ZInputGain',
                'Time',
                'Timestamp',
                'CS0_SpectroscopyChannel',
                'CS0_Parameter',
                'CS0_Direction',
                'CS0_MinimumRawValue',
                'CS0_MaximumRawValue',
                'CS0_MinimumPhysValue',
                'CS0_MaximumPhysValue',
                'CS0_Resolution',
                'CS0_PhysicalUnit',
                'CS0_NumberSpecPoints',
                'CS0_StartPoint',
                'CS0_EndPoint',
                'CS0_Increment',
                'CS0_AcqTimePerPoint',
                'CS0_DelayTimePerPoint',
                'CS0_Feedback',
                'CS0_Filename',
                'CS0_DisplayName',
                'SpecParam',
                'SpecParamRampSpeedEnabled',
                'SpecParamT1us',
                'SpecParamT2us',
                'SpecParamT3us',
                'SpecParamT4us',
                'AFMexists',
                'Amplitude',
                'Deltafsign',
                'otherplace',
                'puntainstabile',
                'noise',
                'TF1_TopographicChannel',
                'TF1_Direction',
                'TF1_MinimumRawValue',
                'TF1_MaximumRawValue',
                'TF1_MinimumPhysValue',
                'TF1_MaximumPhysValue',
                'TF1_Resolution',
                'TF1_PhysicalUnit',
                'TF1_Filename',
                'TF1_DisplayName',
                'CS1_SpectroscopyChannel',
                'CS1_Parameter',
                'CS1_Direction',
                'CS1_MinimumRawValue',
                'CS1_MaximumRawValue',
                'CS1_MinimumPhysValue',
                'CS1_MaximumPhysValue',
                'CS1_Resolution',
                'CS1_PhysicalUnit',
                'CS1_NumberSpecPoints',
                'CS1_StartPoint',
                'CS1_EndPoint',
                'CS1_Increment',
                'CS1_AcqTimePerPoint',
                'CS1_DelayTimePerPoint',
                'CS1_Feedback',
                'CS1_Filename',
                'CS1_DisplayName',
                'TB1_TopographicChannel',
                'TB1_Direction',
                'TB1_MinimumRawValue',
                'TB1_MaximumRawValue',
                'TB1_MinimumPhysValue',
                'TB1_MaximumPhysValue',
                'TB1_Resolution',
                'TB1_PhysicalUnit',
                'TB1_Filename',
                'TB1_DisplayName',
                'Delay',
                'SF0_SpectroscopyChannel',
                'SF0_Parameter',
                'SF0_Direction',
                'SF0_MinimumRawValue',
                'SF0_MaximumRawValue',
                'SF0_MinimumPhysValue',
                'SF0_MaximumPhysValue',
                'SF0_Resolution',
                'SF0_PhysicalUnit',
                'SF0_NumberSpecPoints',
                'SF0_StartPoint',
                'SF0_EndPoint',
                'SF0_Increment',
                'SF0_AcqTimePerPoint',
                'SF0_DelayTimePerPoint',
                'SF0_Feedback',
                'SF0_Filename',
                'SF0_DisplayName',
                'SF1_SpectroscopyChannel',
                'SF1_Parameter',
                'SF1_Direction',
                'SF1_MinimumRawValue',
                'SF1_MaximumRawValue',
                'SF1_MinimumPhysValue',
                'SF1_MaximumPhysValue',
                'SF1_Resolution',
                'SF1_PhysicalUnit',
                'SF1_NumberSpecPoints',
                'SF1_StartPoint',
                'SF1_EndPoint',
                'SF1_Increment',
                'SF1_AcqTimePerPoint',
                'SF1_DelayTimePerPoint',
                'SF1_Feedback',
                'SF1_Filename',
                'SF1_DisplayName',
                'SB0_SpectroscopyChannel',
                'SB0_Parameter',
                'SB0_Direction',
                'SB0_MinimumRawValue',
                'SB0_MaximumRawValue',
                'SB0_MinimumPhysValue',
                'SB0_MaximumPhysValue',
                'SB0_Resolution',
                'SB0_PhysicalUnit',
                'SB0_NumberSpecPoints',
                'SB0_StartPoint',
                'SB0_EndPoint',
                'SB0_Increment',
                'SB0_AcqTimePerPoint',
                'SB0_DelayTimePerPoint',
                'SB0_Feedback',
                'SB0_Filename',
                'SB0_DisplayName',
                'SB1_SpectroscopyChannel',
                'SB1_Parameter',
                'SB1_Direction',
                'SB1_MinimumRawValue',
                'SB1_MaximumRawValue',
                'SB1_MinimumPhysValue',
                'SB1_MaximumPhysValue',
                'SB1_Resolution',
                'SB1_PhysicalUnit',
                'SB1_NumberSpecPoints',
                'SB1_StartPoint',
                'SB1_EndPoint',
                'SB1_Increment',
                'SB1_AcqTimePerPoint',
                'SB1_DelayTimePerPoint',
                'SB1_Feedback',
                'SB1_Filename',
                'SB1_DisplayName',
                'Incomplete',
                'Tags']
        meta = dict(zip(keys, values))
        return meta

    def _set_multiple_tags(self, key, value, tags):
        return self._sql_set(key, value, "Tags", tags)
    
    def _sql_encode(self, sql):
        sql_encoded = str(sql).replace('"', '---')
        sql_encoded = sql_encoded.replace(' ', '__')
        return sql_encoded
