
# Import system modules
import arcpy
import os
import sys

arcpy.env.overwriteOutput = True

# Read the parameter values
# inputFolder = arcpy.GetParameterAsText(0)
# outputFolder = arcpy.GetParameterAsText(1)
inputFolder = "D:\\MSc Thesis\\CCAR Database\\1991\\Resilience_1991_Input"
outputFolder = "D:\\MSc Thesis\\CCAR Database\\1991\\Resilience_1991_Output"

# Define duplicate naming function using variables of name and num
def duplicated_name(name, num):
    # Automate naming conventions
    return name + "_%d"%num

# Define duplicate function using variables of name, inputPath, outputFolder, numCopies
def duplicate(name, inputPath, outputFolder, numCopies):
    # Loop for numCopies times where x is in range of 0 - numCopies
    for x in range(0, numCopies):
        # Create fcName variable by importing duplicate_name function
        fcName = duplicated_name(name, x)
        # Automate copy feature function with
        arcpy.CopyFeatures_management(inputPath, outputFolder + "\\" + fcName)

# Define delete field and rename feature class function using variables of path, keepFields, newName
def delete_rename(path, keepFields, newName):
    # Automate list fields
    fields = arcpy.ListFields(path)
    # Automate drop fields
    dropFields = [x.name for x in fields if x.name not in keepFields]
    # Automate delete field
    arcpy.DeleteField_management(path, dropFields)
    # Automate rename
    arcpy.Rename_management(path, newName)

# Define feature class directory function using variable of InputFolder
def fcs_in_workspace(inputFolder):
    # Direct to output folder
    if not os.path.exists(outputFolder):
        os.makedirs(outputFolder)
    # Loop for root, subFolders, files in inputFolder
    suffix = ".shp"
    for root, subFolders, files in os.walk(inputFolder):
        # Loop for fileName in files
        for fileName in files:
            if fileName.endswith(suffix):
                # define environment workspace
                arcpy.env.workspace = root
                # List feature classes
                fclist = arcpy.ListFeatureClasses()
                # Loop for fc in fclist
                for fc in fclist:
                    # Describe fc name
                    name = arcpy.Describe(root + "\\" +fc).name.strip(".shp")
                    if name == "Income_EA":
                        duplicate(name, fc, outputFolder, 2)
                    elif name == "Dwelling_households_B_EA":
                        duplicate(name, fc, outputFolder, 3)

    # Change environment workspace within the loop
    arcpy.env.workspace = outputFolder
    # List feature classes
    fclist = arcpy.ListFeatureClasses()
    # Loop for fc in fclist
    for fc in fclist:
        # Define path parameter for delete_rename function
        path = outputFolder + "\\" + fc
        name = arcpy.Describe(path).name.strip(".shp")
        # Pre-declare variables of keepFeilds & newName with empty container for delete_rename function
        # Allow to let the lower tier block of different keepFields & newName codes
        # Fill in the upper tier keepFields & newName
        keepFields = []
        newName = ""

        # Import duplicated_name function to define names for if conditions
        if name == duplicated_name("Income_EA", 0):
            # Define keepField & newName parameters for delete_rename function
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL60"]
            newName = "Household_Average_Income_Income_1991_DA.shp"
        elif name == duplicated_name("Income_EA", 1):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL54"]
            newName = "Low_Income_Income_1991_DA.shp"
        elif name == duplicated_name("Dwelling_households_B_EA", 0):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL9"]
            newName = "Dwellings_Average_Value_Dwellings_1991_DA.shp"
        elif name == duplicated_name("Dwelling_households_B_EA", 1):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL34"]
            newName = "Lone_Parent_Dwellings_1991_DA.shp"
        elif name == duplicated_name("Dwelling_households_B_EA", 2):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL51"]
            newName = "Live_Alone_Dwellings_1991_DA.shp"

        else:
            continue

        #  Import delete_rename function only once instead execute in every lower tier if condition
        delete_rename(path, keepFields, newName)

if __name__ == '__main__':
    fcs_in_workspace(inputFolder)
