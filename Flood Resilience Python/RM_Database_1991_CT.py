
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
    return name + "_%d" % num

# Define duplicate function using variables of name, inputPath, outputFolder, numCopies
def duplicate(name, inputPath, outputFolder, numCopies):
    # Loop for numCopies times where x is in range of 0 - numCopies
    for x in range(0, numCopies):
        # Create fcName variable by importing duplicate_name function
        fcName = duplicated_name(name, x)
        # Automate copy feature function
        arcpy.CopyFeatures_management(inputPath, outputFolder + "\\" + fcName)

# Define null value identification process with variable of path
def null_identify(path):
    # Automate add transformation variable field
    fieldName0 = arcpy.ValidateFieldName("Tvariable")
    arcpy.AddField_management(path, fieldName0, "TEXT", "", "", 50)
    # Automate field calculator for normalized variable
    arcpy.CalculateField_management(path, fieldName0, '""', "PYTHON_9.3")

# Define data process function using variables of path, keepFields, expression, newName
def data_process(path, keepFields, expression0, expression1, newName):
    # Automate add normalized variable field
    fieldName1 = arcpy.ValidateFieldName("TFvariable")
    arcpy.AddField_management(path, fieldName1, "DOUBLE", "", "", 50)
    # Automate field calculator for normalized variable
    arcpy.CalculateField_management(path, fieldName1, expression0, "PYTHON_9.3")
    # Automate add normalized variable field
    fieldName2 = arcpy.ValidateFieldName("Z_score")
    arcpy.AddField_management(path, fieldName2, "DOUBLE", "", "", 50)
    # Automate field calculator for normalized variable
    arcpy.CalculateField_management(path, fieldName2, expression1, "PYTHON_9.3")
    # Automate list fields
    fields = arcpy.ListFields(path)
    # Automate drop fields
    dropFields = [x.name for x in fields if x.name not in keepFields]
    # Automate delete field
    arcpy.DeleteField_management(path, dropFields)
    # Automate rename
    arcpy.Rename_management(path, newName)

# Define field process function using variables of path, fieldName0, exp0, fieldName1, exp1
def field_process(path, fieldName0, fieldName1, exp0, exp1):
    arcpy.AddField_management(path, fieldName0, "DOUBLE", "", "", 50)
    arcpy.CalculateField_management(path, fieldName0, exp0, "PYTHON_9.3")
    arcpy.AddField_management(path, fieldName1, "DOUBLE", "", "", 50)
    arcpy.CalculateField_management(path, fieldName1, exp1, "PYTHON_9.3")

# Define variable calculate function using variables of path, fieldName
def variable_calculate(path, fieldName):
    statsfield = arcpy.da.FeatureClassToNumPyArray(path, fieldName)
    u = statsfield[fieldName].mean()
    sd = statsfield[fieldName].std()
    return "(!%s! - %f) / %f" % (fieldName, u, sd)
    # factor = u / sd
    # return "!%s! * %f" % (fieldName, factor)

# Define null value process function using variables of path
def null_process(path):
    name = arcpy.Describe(path).name.strip(".shp")
    if name == duplicated_name("Citizen_Immigration_Migration_CT", 0):
        null_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["COL4", "Tvariable"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                row[0] = 1
                row[1] = "Estimated"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Citizen_Immigration_Migration_CT", 1):
        null_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["COL7", "Tvariable"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                row[0] = 1
                row[1] = "Estimated"
                cursor.updateRow(row)
        del row
        del cursor

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
                    if name == "Citizen_Immigration_Migration_CT":
                        duplicate(name, fc, outputFolder, 2)

    # Change environment workspace within the loop
    arcpy.env.workspace = outputFolder
    # List feature classes
    fclist = arcpy.ListFeatureClasses()
    # Loop for fc in fclist
    for fc in fclist:
        # Define path parameter for delete_rename function
        path = outputFolder + "\\" + fc
        name = arcpy.Describe(path).name.strip(".shp")
        null_process(path)
        arcpy.AddField_management(path, "normalize", "DOUBLE", "", "", 50)
        # Pre-declare variables of keepFields & newName with empty container for delete_rename function
        # Allow to let the lower tier block of different keepFields & newName codes
        # Fill in the upper tier keepFields & newName
        keepFields = []
        expression0 = ""
        expression1 = ""
        newName = ""

        # Import duplicated_name function to define names for if conditions
        if name == duplicated_name("Citizen_Immigration_Migration_CT", 0):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL4", "COL63",  "Tvariable", "TFvariable", "Z_score"]
            # arcpy.AddField_management(path, "COL4_", "DOUBLE", "", "", 50)
            # arcpy.CalculateField_management(path, "COL4_", "!COL4!", "PYTHON_9.3")
            # arcpy.AddField_management(path, "COL63_", "DOUBLE", "", "", 50)
            # arcpy.CalculateField_management(path, "COL63_", "!COL63!", "PYTHON_9.3")
            field_process(path, "COL4_", "COL63_", "!COL4!", "!COL63!")
            expression0 = "!COL63_! / !COL4_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Immigration_Citizen_1991_CT.shp"
        elif name == duplicated_name("Citizen_Immigration_Migration_CT", 1):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL7", "COL42", "Tvariable", "TFvariable", "Z_score"]
            field_process(path, "COL7_", "COL42_", "!COL7!", "!COL42!")
            expression0 = "!COL42_! / !COL7_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Canadian_Citizen_Citizen_1991_CT.shp"
        else:
            continue

        # Import delete_rename function only once instead execute in every lower tier if condition
        data_process(path, keepFields, expression0, expression1, newName)

if __name__ == '__main__':
    fcs_in_workspace(inputFolder)
