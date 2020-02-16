
# Import system modules
import arcpy
from arcpy import env
import os
import sys
import numpy


# Overwrite Output
arcpy.env.overwriteOutput = True

# Read the parameter values
# InputFolder = arcpy.GetParameterAsText(0)
# OutputFolder = arcpy.GetParameterAsText(1)
InputFolder = "D:\\MSc Thesis\\CCAR Database\\1991\\Resilience_1991_Input"
OutputFolder = "D:\\MSc Thesis\\CCAR Database\\1991\\Resilience_1991_Output"

# Creating Functions
def fcs_in_workspace(InputFolder):

    # Create output folder
    if not os.path.exists(OutputFolder):
        os.makedirs(OutputFolder)

    # Create loop for copy feature
    suffix = ".shp"
    for root, subFolders, files in os.walk(InputFolder):
        for fileName in files:
            if fileName.endswith(suffix):
                arcpy.env.workspace = root
                fclist = arcpy.ListFeatureClasses()
                for fc in fclist:
                    fc_copy = os.path.join(OutputFolder, fc.strip(".shp"))
                    fc_path = os.path.join(OutputFolder, fc)
                    # fc must be start with upper case character to copy feature correctly
                    arcpy.CopyFeatures_management(fc, fc_copy)

                    # Describe copy featured shapefiles
                    desc = arcpy.Describe(fc_path)

                    # if copy feature is _____
                    if desc.name == "Income_EA.shp":
                        # List the copy feature attribute fields
                        fields = arcpy.ListFields(fc_path)
                        # Manually enter field names to keep here
                        # include mandatory fields name in keepfields
                        keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL60"]
                        # Automatically drop fields
                        dropFields = [x.name for x in fields if x.name not in keepFields]
                        # Delete fields
                        arcpy.DeleteField_management(fc_path, dropFields)
                        # Converts a feature class to NumPy structured array.
                        statsfield = arcpy.da.FeatureClassToNumPyArray(fc_path, ("COL60"))
                        u = statsfield["COL60"].mean()
                        sd = statsfield["COL60"].std()
                        factor = u / sd
                        # Add z score field
                        fieldname = arcpy.ValidateFieldName("z_score")
                        arcpy.AddField_management(fc_path, fieldname, "DOUBLE", "", "", 50)
                        # Calculate z score Field
                        # Change "factor" into string to feed calculate field (new format available)
                        # print("factor: %f\n" % factor)
                        expression = "!COL60! * %f" % factor
                        arcpy.CalculateField_management(fc_path, fieldname, expression, "PYTHON_9.3")
                        # Rename
                        arcpy.Rename_management(fc_path, "Household_Average_Income_Income_1991_DA.shp")


if __name__ == '__main__':
    fcs_in_workspace(InputFolder)

