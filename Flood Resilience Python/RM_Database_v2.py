# Import system modules
import arcpy
import os
import sys

arcpy.env.overwriteOutput = True

# Read the parameter values
# InputFolder = arcpy.GetParameterAsText(0)
# OutputFolder = arcpy.GetParameterAsText(1)
InputFolder = "D:\\MSc Thesis\\CCAR Database\\1991\\Resilience_1991_Input"
OutputFolder = "D:\\MSc Thesis\\CCAR Database\\1991\\Resilience_1991_Output"

# copy a
def copy_feature():


# define feature class directory function
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
                # creating loop in fclist
                for fc in fclist:
                    fc_copy = os.path.join(OutputFolder, fc.strip(".shp"))
                    fc_path = os.path.join(OutputFolder, fc)
                    # must make a existing copy in order to describe
                    arcpy.CopyFeatures_management(fc, fc_copy)
                    desc = arcpy.Describe(fc_path)

                    if desc.name == "Income_EA.shp":
                        # make function variables
                        fc_copy1 = os.path.join(OutputFolder, fc.strip(".shp") + "_1")
                        fc_path1 = os.path.join(OutputFolder, fc_copy1 + ".shp")

                        # copy features before changing original fc copy
                        # fc must be start with upper case character to copy feature correctly
                        arcpy.CopyFeatures_management(fc_path, fc_copy1)

                        # List the copy feature attribute fields
                        fields = arcpy.ListFields(fc_path)
                        # Manually enter field names to keep here
                        # include mandatory fields name in keepfields
                        keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL_60"]
                        # Automatically drop fields
                        dropFields = [x.name for x in fields if x.name not in keepFields]
                        # Delete fields
                        arcpy.DeleteField_management(fc_path, dropFields)
                        # Rename
                        arcpy.Rename_management(fc_path, "Household_Average_Income_Income_1991_DA.shp")

                        # List the copy feature attribute fields
                        fields = arcpy.ListFields(fc_path1)
                        # Manually enter field names to keep here
                        # include mandatory fields name in keepfields
                        keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL_54"]
                        # Automatically drop fields
                        dropFields = [x.name for x in fields if x.name not in keepFields]
                        # Delete fields
                        arcpy.DeleteField_management(fc_path1, dropFields)
                        # Rename
                        arcpy.Rename_management(fc_path1, "Low_Income_Income_1991_DA.shp")

                    elif desc.name == "Dwelling_households_B_EA.shp":
                        # make function variables
                        fc_copy2 = os.path.join(OutputFolder, fc.strip(".shp") + "_1")
                        fc_copy3 = os.path.join(OutputFolder, fc.strip(".shp") + "_2")
                        fc_path2 = os.path.join(OutputFolder, fc_copy2 + ".shp")
                        fc_path3 = os.path.join(OutputFolder, fc_copy3 + ".shp")

                        # copy feature
                        # fc must be start with upper case character to copy feature correctly
                        arcpy.CopyFeatures_management(fc, fc_copy2)
                        arcpy.CopyFeatures_management(fc, fc_copy3)

                        # List the copy feature attribute fields
                        fields = arcpy.ListFields(fc_path)
                        # Manually enter field names to keep here
                        # include mandatory fields name in keepfields
                        keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL_9"]
                        # Automatically drop fields
                        dropFields = [x.name for x in fields if x.name not in keepFields]
                        # Delete fields
                        arcpy.DeleteField_management(fc_path, dropFields)
                        # Rename
                        arcpy.Rename_management(fc_path, "Dwellings_Average_Value_Dwellings_1991_DA.shp")

                        # List the copy feature attribute fields
                        fields = arcpy.ListFields(fc_path2)
                        # Manually enter field names to keep here
                        # include mandatory fields name in keepfields
                        keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL_34"]
                        # Automatically drop fields
                        dropFields = [x.name for x in fields if x.name not in keepFields]
                        # Delete fields
                        arcpy.DeleteField_management(fc_path2, dropFields)
                        # Rename
                        arcpy.Rename_management(fc_path2, "Lone_Parent_Dwellings_1991_DA.shp")

                        # List the copy feature attribute fields
                        fields = arcpy.ListFields(fc_path3)
                        # Manually enter field names to keep here
                        # include mandatory fields name in keepfields
                        keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL_51"]
                        # Automatically drop fields
                        dropFields = [x.name for x in fields if x.name not in keepFields]
                        # Delete fields
                        arcpy.DeleteField_management(fc_path3, dropFields)
                        # Rename
                        arcpy.Rename_management(fc_path3, "Live_Alone_Dwellings_1991_DA.shp")
if __name__ == '__main__':
    fcs_in_workspace(InputFolder)
