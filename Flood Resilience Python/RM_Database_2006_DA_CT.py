
# Import system modules
import arcpy
import os
import sys

arcpy.env.overwriteOutput = True

# Read the parameter values
# inputFolder = arcpy.GetParameterAsText(0)
# outputFolder = arcpy.GetParameterAsText(1)
inputFolder = "D:\\MSc Thesis\\CCAR Database\\2006\\Social_Resilience_2006_Input"
outputFolder = "D:\\MSc Thesis\\CCAR Database\\2006\\Social_Resilience_2006_Output"


# Define duplicate naming function using variables of name and num
def duplicated_name(name, num):
    # Automate naming conventions
    return name + "_%d" % num
# Define duplicate naming function using variables of name and num0, num1
def duplicated_name1(name, num0, num1):
    # Automate naming conventions
    return name + "_%d_%d" % (num0, num1)

# Define duplicate function using variables of name, inputPath, outputFolder, numCopies
def duplicate(name, inputPath, outputFolder, numCopies):
    # Loop for numCopies times where x is in range of 0 - numCopies
    for x in range(0, numCopies):
        # Create fcName variable by importing duplicate_name function
        fcName = duplicated_name(name, x)
        # Automate copy feature function
        arcpy.CopyFeatures_management(inputPath, outputFolder + "\\" + fcName)

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

# Define value calculate function using variables of path, name, x, y, fieldName
def value_calculate(path, name, x, y, fieldName):
    # Create fcName variable by importing duplicate_name function
    fcName = duplicated_name1(name, x, y)
    # Automate copy feature function
    arcpy.CopyFeatures_management(path, outputFolder + "\\" + fcName)
    path1 = outputFolder + "\\" + fcName + ".shp"
    cursor = arcpy.da.UpdateCursor(path1, [fieldName])
    for row in cursor:
        if row[0] == -9999:
            cursor.deleteRow()
    del row
    del cursor
    statsfield = arcpy.da.FeatureClassToNumPyArray(path1, fieldName)
    u = statsfield[fieldName].mean()
    arcpy.Delete_management(path1)
    return u

# Define value process function using variables of path
def value_process(path):
    # Automate add transformation variable field
    fieldName0 = arcpy.ValidateFieldName("Tvariable")
    arcpy.AddField_management(path, fieldName0, "TEXT", "", "", 50)
    # Automate field calculator for normalized variable
    arcpy.CalculateField_management(path, fieldName0, '""', "PYTHON_9.3")
    name = arcpy.Describe(path).name.strip(".shp")
    if name == duplicated_name("Income_DA", 0):
        u0 = value_calculate(path, "Income_DA", 0, 1, "AVERAGE_HO")
        cursor = arcpy.da.UpdateCursor(path, ["POPULATION", "AVERAGE_HO", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
            if row[0] == -9999:
                row[1] = u0
                row[2] = "N/A"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Labour_DA", 0):
        u0 = value_calculate(path, "Labour_DA", 0, 1, "IN_THE_LAB")
        u1 = value_calculate(path, "Labour_DA", 0, 2, "UNEMPLOYED")
        cursor = arcpy.da.UpdateCursor(path, ["IN_THE_LAB", "UNEMPLOYED", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
            if row[0] == -9999:
                row[0] = u0
                cursor.updateRow(row)
            if row[1] == -9999:
                row[1] = u1
                row[2] = "N/A"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Marital_DA", 0):
        u0 = value_calculate(path, "Marital_DA", 0, 1, "TOTAL_NU8")
        u1 = value_calculate(path, "Marital_DA", 0, 2, "PERIOD_OF_")
        u2 = value_calculate(path, "Marital_DA", 0, 3, "PERIOD_OF1")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU8", "PERIOD_OF_", "PERIOD_OF1", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
            if row[0] == -9999:
                row[0] = u0
                cursor.updateRow(row)
            if row[1] == -9999 or row[2] == -9999:
                row[1] = u1
                row[2] = u2
                row[3] = "N/A"
                cursor.updateRow(row)
        del row
        del cursor

# Define field process function using variables of path, fieldName0, exp0
def field_process0(path, fieldName0, exp0):
    arcpy.AddField_management(path, fieldName0, "DOUBLE", "", "", 50)
    arcpy.CalculateField_management(path, fieldName0, exp0, "PYTHON_9.3")

# Define field process function using variables of path, fieldName0, exp0, fieldName1, exp1
def field_process1(path, fieldName0, fieldName1, exp0, exp1):
    arcpy.AddField_management(path, fieldName0, "DOUBLE", "", "", 50)
    arcpy.CalculateField_management(path, fieldName0, exp0, "PYTHON_9.3")
    arcpy.AddField_management(path, fieldName1, "DOUBLE", "", "", 50)
    arcpy.CalculateField_management(path, fieldName1, exp1, "PYTHON_9.3")

# Define field process function using variables of path, fieldName0, exp0, fieldName1, exp1, fieldName2, exp2
def field_process2(path, fieldName0, fieldName1, fieldName2, exp0, exp1, exp2):
    arcpy.AddField_management(path, fieldName0, "DOUBLE", "", "", 50)
    arcpy.CalculateField_management(path, fieldName0, exp0, "PYTHON_9.3")
    arcpy.AddField_management(path, fieldName1, "DOUBLE", "", "", 50)
    arcpy.CalculateField_management(path, fieldName1, exp1, "PYTHON_9.3")
    arcpy.AddField_management(path, fieldName2, "DOUBLE", "", "", 50)
    arcpy.CalculateField_management(path, fieldName2, exp2, "PYTHON_9.3")

def variable_calculate(path, fieldName):
    statsfield = arcpy.da.FeatureClassToNumPyArray(path, fieldName)
    u = statsfield[fieldName].mean()
    sd = statsfield[fieldName].std()
    return "(!%s! - %f) / %f" % (fieldName, u, sd)

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
                    if name == "Income_DA":
                        duplicate(name, fc, outputFolder, 1)
                    elif name == "Labour_DA":
                        duplicate(name, fc, outputFolder, 1)
                    elif name == "Marital_DA":
                        duplicate(name, fc, outputFolder, 1)

    # Change environment workspace within the loop
    arcpy.env.workspace = outputFolder
    # List feature classes
    fclist = arcpy.ListFeatureClasses()
    # Loop for fc in fclist
    for fc in fclist:
        # Define path parameter for delete_rename function
        path = outputFolder + "\\" + fc
        name = arcpy.Describe(path).name.strip(".shp")
        value_process(path)
        arcpy.AddField_management(path, "normalize", "DOUBLE", "", "", 50)
        # Pre-declare variables of keepFields & newName with empty container for delete_rename function
        # Allow to let the lower tier block of different keepFields & newName codes
        # Fill in the upper tier keepFields & newName
        keepFields = []
        expression0 = ""
        expression1 = ""
        newName = ""

        # Import duplicated_name function to define names for if conditions
        if name == duplicated_name("Income_DA", 0):
            # Define keepField & newName parameters for delete_rename function
            keepFields = ["FID", "Shape", "DAUID", "CSDUID", "CCSUID", "CDUID", "ERUID", "PRUID", "CTUID", "CMAUID", "OID_", "GEOGRAPHY", "AVERAGE_HO", "POPULATION", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!AVERAGE_HO!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Household_Average_Income_Income_2006_DA.shp"
        elif name == duplicated_name("Labour_DA", 0):
            keepFields = ["FID", "Shape", "DAUID", "CSDUID", "CCSUID", "CDUID", "ERUID", "PRUID", "CTUID", "CMAUID", "OID_", "GEOGRAPHY", "UNEMPLOYED", "IN_THE_LAB", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!UNEMPLOYED! / !IN_THE_LAB!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Labour_Unemployment_Labour_2006_DA.shp"
        elif name == duplicated_name("Marital_DA", 0):
            keepFields = ["FID", "Shape", "DAUID", "CSDUID", "CCSUID", "CDUID", "ERUID", "PRUID", "CTUID", "CMAUID", "OID_", "GEOGRAPHY", "PERIOD_OF_", "PERIOD_OF1", "TOTAL_NU8", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "(!PERIOD_OF_! + !PERIOD_OF1!) / !TOTAL_NU8!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_1960Constructions_Marital_2006_DA.shp"
        else:
            continue

        # Import delete_rename function only once instead execute in every lower tier if condition
        data_process(path, keepFields, expression0, expression1, newName)

if __name__ == '__main__':
    fcs_in_workspace(inputFolder)
