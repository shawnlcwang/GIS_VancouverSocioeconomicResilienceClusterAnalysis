Readme - AZT Importer
v1.0.1 20/10/10

This tool takes an ESRI Shapefile containing polygon data, and generates the corresponding AAT and PAT files for use by the AZTool_M software. These files are similar to those produced from an ESRI coverage dataset, but with some differences resulting from the Shapefile format and conversion process.

The outputs are comma delimited, and headers are included in the PAT attribute file.
 * An AZM_ID attribute is added that is the FID of each feature, this corresponds to the ID used in the AAT contiguity file.
 * An AZM_Area attribute is also added, containing the area of each polygon in coordinate system units. 

A row for the External Polygon is generated, but is assigned an ID one greater than the last FID value of the data rather than 1 which would be the ID assigned by ArcGIS if the PAT/AAT files were generated from a coverage. The area value for the External Polygon is set to -1 to flag it as such.

The tool can be called from the command line using the following syntax:

AZTImport.exe InputSHPFile OutputAATfile

Both arguments are expected to have full paths, eg c:\temp\AZTool\Southampton.shp rather than AZTool\Southampton.shp