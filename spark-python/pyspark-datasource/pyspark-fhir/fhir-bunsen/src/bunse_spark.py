from bunsen.stu3.codes import create_hierarchies
from bunsen.codes.loinc import with_loinc_hierarchy
from bunsen.codes.snomed import with_relationships

# Add SNOMED to the value sets
snomed_hierarchy= with_relationships(
      spark,
      create_hierarchies(spark),
      '/path/to/mappings/snomedct_rf2/20160901/Snapshot/Terminology/sct2_Relationship_Snapshot_US1000124_20160901.txt',
      '20160901')

# Add LOINC to the value sets
loinc_hierarchy= with_loinc_hierarchy(
       spark,
       snomed_hierarchy,
       '/path/to/mappings/loinc_hierarchy/2.56/LOINC_2.56_MULTI-AXIAL_HIERARCHY.CSV',
       '2.56')

# Make sure that a database called `ontologies` is created before writing to the database.
spark.sql("CREATE DATABASE IF NOT EXISTS ontologies")

# Write the SNOMED and LOINC data to the ontologies database, where it is visible
# in Bunsen's valueset functions.
loinc_hierarchy.write_to_database('ontologies')
snomed_hierarchy.write_to_database('ontologies')