import MySQLdb 
import os

HOST = os.environ.get('DWH_HOST')
USER = os.environ.get('DWH_USER')
PASS = os.environ.get('DWH_PASS')
"""name: oci_dwh_connect
input: none
output: none 
function: connects to the OCI DWH so that mysql queries can be run against it.
requirements: python 2.6 or above 
              environment variables DHW_HOST defined with the host of the OCI DWH
              DWH_USER should be set to dwh
              DWH_PASS should be set to sanscreen per OCI RO user standards
              os module
              sys module
              MySQLdb module"""

              
class oci_dwh_connect(object):
     """initialization 
     input: none
     output: none 
     function: connect to dwh, establish cursor"""
     def __init__(self):
        conn = MySQLdb.connect(user=USER, passwd=PASS, host=HOST)
        self.cursor = conn.cursor()
    
     """name: query
     input: MySQL query
     output: Results of query in tupels"""
     def query(self, query):
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        return result
 
     """name: get_storage_with_no_dept
     input: none
     output: all storage with no dept # assigned in a list of tupels"""
     def get_storage_with_no_dept(self):
        query = (
           'SELECT b.Internal_Volume, TRIM(leading "/" FROM b.Qtree) as Qtree \
           FROM tmp.bu b WHERE Department_Number LIKE "N/A" \
           OR Department_Number IS null;')
        return self.query(query)
  
     """name: verbose_storage_with_no_dept
     input: none
     output: all volumes with no dept number, and the storage those volumes reside on."""
     def verbose_storage_with_no_dept(self):
        query = (
           'SELECT i.id, b.Storage, b.Internal_Volume, \
           TRIM(leading "/" FROM b.Qtree) as Qtree \
           FROM tmp.bu b LEFT JOIN dwh_inventory.internal_volume i ON \
           i.name = b.internal_volume WHERE b.Department_Number \
           LIKE "N/A" OR b.Department_Number IS null ORDER BY i.id')
        return self.query(query)

     """name: get_vmdk_storage
     input: none
     output: all volumes in OCI used for VMware"""
     def get_vmdk_storage(self):
        query =(
            "SELECT * FROM tmp.bu \
            WHERE Internal_Volume like '%ABSN%'")
        return self.query(query)

     """name: get_ws_info
     input: userid
     output: information about the qtree(s) named after a user id (or a workspace)"""
     def get_ws_info(self, user):
        query =(
            "SELECT q.identifier, q.name, q.quotaHardCapacityLimitMB, q.quotaUsedCapacityMB \
            FROM dwh_inventory.qtree q \
            WHERE q.identifier like '%wslocal%' AND q.name = '{0}'".format(user))
        return self.query(query)
   
     """name: get_quota_by_volume
     input: volume
     output: get all the quotas for a given volume."""
     def get_quota_by_volume(self, volume):
        query = (
            "SELECT identifier, targetUser \
            FROM dwh_inventory.quota \
            WHERE identifier LIKE '{0}'".format(volume))
        return self.query(query)

     """name: get_vol_info
     input: cluster name, volume name
     output: all the volume information for a spacific volume in OCI"""
     def get_vol_info(self, cluster, volume):
        query = (
              "SELECT * FROM dwh_inventory.internal_volume \
              WHERE identifier LIKE '%{0}' \
              AND identifier LIKE {1}%".format(cluster, volume)) 
        return self.query(query)

     """name: get_snapmirror_relationships_via_source
     input: source volume
     output: the destination volumes of all the snapmirror relationships for the source volume"""
     def get_snapmirror_relationships_via_source(self, source):
        query = (
              "SELECT iv1.name AS source_volume, iv1.totalAllocatedCapacityMB, iv1.dataUsedCapacityMB, iv2.name AS dest_volume, iv2.totalAllocatedCapacityMB, iv2.dataUsedCapacityMB \
              FROM dwh_inventory.dr_internal_volume_replica divr \
              JOIN dwh_inventory.internal_volume iv1 ON iv1.id = divr.sourceInternalVolumeId \
              JOIN dwh_inventory.internal_volume iv2 ON iv2.id = divr.targetInternalVolumeId \
              WHERE iv1.name LIKE '%{0}%' \
              ORDER BY iv1.name ASC".format(source)) 
        return self.query(query)
