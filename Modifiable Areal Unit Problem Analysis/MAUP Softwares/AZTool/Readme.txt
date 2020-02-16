Readme - AZTool exe and Demo data
26/8/11

Directory Contents:

AZTool_M.exe
The AZTool executable. Requires .NET framework v2 to be installed to run.

AZTool_M_Overview.doc
A description of the processing performed by the tool, along with the effect of the various parameters passed in the XML parameter file

AZTool_M_Parameters.xml
A tool parameter file for the bbhom2 demo dataset

bbhom2.aat
Demo data contiguity file

bbhom2.pat
Demo data attribute file - this contains the following columns of data:
ID, Population, Area, OwnOcc, PrRent, HARent, Det, Semi, Flat
The last six columns are homogeneity variables, split into two groups of three counts, the first is Tenure, the second Accommodation Type.

Run_AZTool_M.bat
Batch file demonstrating the command line arguments supported by the tool

SHP (Directory)
The demo data in ESRI Shapefile format


The tool is set up to use the bbhom2 demo dataset provided. As the tool does not support relative paths at the moment, the batch file and parameter file are set to use the C:\AZT_Demo\ directory as the location for the input and output data.