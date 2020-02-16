
# Import system modules
import arcpy
import os
import sys

arcpy.env.overwriteOutput = True

# Read the parameter values
# inputFolder = arcpy.GetParameterAsText(0)
# outputFolder = arcpy.GetParameterAsText(1)
inputFolder = "D:\\MSc Thesis\\CCAR Database\\2011\\Social_Resilience_2011_Input"
outputFolder = "D:\\MSc Thesis\\CCAR Database\\2011\\Social_Resilience_2011_Output"


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
    if name == duplicated_name("Income_DA_", 0):
        cursor = arcpy.da.UpdateCursor(path, ["POPULATI_1", "AVERAGE_HO", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Income_DA_", 1):
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU4", "RENTER", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Income_DA_", 2):
        cursor = arcpy.da.UpdateCursor(path, ["POPULATI_1", "PREVALENCE", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Income_DA_", 3):
        cursor = arcpy.da.UpdateCursor(path, ["POPULATI_1", "GOVERNMENT", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Income_DA_", 4):
        cursor = arcpy.da.UpdateCursor(path, ["POPULATI_1", "AVERAGE_VA", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Income_DA_", 5):
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NUMB", "MAJOR_REPA", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Income_DA_", 6):
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NUM1", "1960_OR_BE", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Marital_Language_DA", 0):
        u0 = value_calculate(path, "Marital_Language_DA", 0, 1, "KNOWLEDGE_")
        u1 = value_calculate(path, "Marital_Language_DA", 0, 2, "NEITHER_EN")
        cursor = arcpy.da.UpdateCursor(path, ["KNOWLEDGE_", "NEITHER_EN", "Tvariable"])
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
    elif name == duplicated_name("Marital_Language_DA", 1):
        u0 = value_calculate(path, "Marital_Language_DA", 1, 1, "TOTAL_NUMB")
        u1 = value_calculate(path, "Marital_Language_DA", 1, 2, "TOTAL_LONE")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NUMB", "TOTAL_LONE", "Tvariable"])
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
    elif name == duplicated_name("Marital_Language_DA", 2):
        u0 = value_calculate(path, "Marital_Language_DA", 2, 1, "TOTAL_NU3")
        u1 = value_calculate(path, "Marital_Language_DA", 2, 2, "LIVING_AL1")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU3", "LIVING_AL1", "Tvariable"])
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
    elif name == duplicated_name("Marital_Language_DA", 3):
        u0 = value_calculate(path, "Marital_Language_DA", 3, 1, "TOTAL_NU5")
        u1 = value_calculate(path, "Marital_Language_DA", 3, 2, "MOVABLE_DW")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU5", "MOVABLE_DW", "Tvariable"])
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

    elif name == duplicated_name("Income_Labour_Immigration_CT", 0):
        u0 = value_calculate(path, "Income_Labour_Immigration_CT", 0, 1, "AVERAGE_HO")
        cursor = arcpy.da.UpdateCursor(path, ["POPULATION", "AVERAGE_HO", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
            if row[1] == -9999:
                row[1] = u0
                row[2] = "N/A"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Income_Labour_Immigration_CT", 1):
        u0 = value_calculate(path, "Income_Labour_Immigration_CT", 1, 1, "IN_THE_LAB")
        u1 = value_calculate(path, "Income_Labour_Immigration_CT", 1, 2, "UNEMPLOYED")
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
    elif name == duplicated_name("Income_Labour_Immigration_CT", 2):
        u0 = value_calculate(path, "Income_Labour_Immigration_CT", 2, 1, "TOTAL_NU4")
        u1 = value_calculate(path, "Income_Labour_Immigration_CT", 2, 2, "RENTER")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU4", "RENTER", "Tvariable"])
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
    elif name == duplicated_name("Income_Labour_Immigration_CT", 3):
        u0 = value_calculate(path, "Income_Labour_Immigration_CT", 3, 1, "PREVALENCE")
        cursor = arcpy.da.UpdateCursor(path, ["POPULATION", "PREVALENCE", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
            if row[1] == -9999:
                row[1] = u0
                row[2] = "N/A"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Income_Labour_Immigration_CT", 4):
        u0 = value_calculate(path, "Income_Labour_Immigration_CT", 4, 1, "GOVERNMENT")
        cursor = arcpy.da.UpdateCursor(path, ["POPULATION", "GOVERNMENT", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
            if row[1] == -9999:
                row[1] = u0
                row[2] = "N/A"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Income_Labour_Immigration_CT", 5):
        u0 = value_calculate(path, "Income_Labour_Immigration_CT", 5, 1, "AVERAGE_VA")
        cursor = arcpy.da.UpdateCursor(path, ["POPULATION", "AVERAGE_VA", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
            if row[1] == -9999:
                row[1] = u0
                row[2] = "N/A"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Income_Labour_Immigration_CT", 6):
        u0 = value_calculate(path, "Income_Labour_Immigration_CT", 6, 1, "TOTAL_NUMB")
        u1 = value_calculate(path, "Income_Labour_Immigration_CT", 6, 2, "MAJOR_REPA")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NUMB", "MAJOR_REPA", "Tvariable"])
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
    elif name == duplicated_name("Income_Labour_Immigration_CT", 7):
        u0 = value_calculate(path, "Income_Labour_Immigration_CT", 7, 1, "TOTAL_NUM1")
        u1 = value_calculate(path, "Income_Labour_Immigration_CT", 7, 2, "1960_OR_BE")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NUM1", "1960_OR_BE", "Tvariable"])
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
    elif name == duplicated_name("Income_Labour_Immigration_CT", 8):
        u0 = value_calculate(path, "Income_Labour_Immigration_CT", 8, 1, "TOTAL_POP1")
        u1 = value_calculate(path, "Income_Labour_Immigration_CT", 8, 1, "2006_TO_20")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_POP1", "2006_TO_20", "Tvariable"])
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
    elif name == duplicated_name("Income_Labour_Immigration_CT", 9):
        u0 = value_calculate(path, "Income_Labour_Immigration_CT", 9, 1, "TOTAL_PO_1")
        u1 = value_calculate(path, "Income_Labour_Immigration_CT", 9, 2, "CANADIAN_C")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_PO_1", "CANADIAN_C", "Tvariable"])
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
    elif name == duplicated_name("Marital_Language_CT", 0):
        u0 = value_calculate(path, "Marital_Language_CT", 0, 1, "KNOWLEDGE_")
        u1 = value_calculate(path, "Marital_Language_CT", 0, 2, "NEITHER_EN")
        cursor = arcpy.da.UpdateCursor(path, ["KNOWLEDGE_", "NEITHER_EN", "Tvariable"])
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
    elif name == duplicated_name("Marital_Language_CT", 1):
        u0 = value_calculate(path, "Marital_Language_CT", 1, 1, "TOTAL_NUMB")
        u1 = value_calculate(path, "Marital_Language_CT", 1, 2, "TOTAL_LONE")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NUMB", "TOTAL_LONE", "Tvariable"])
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
    elif name == duplicated_name("Marital_Language_CT", 2):
        u0 = value_calculate(path, "Marital_CT", 2, 1, "TOTAL_NU3")
        u1 = value_calculate(path, "Marital_CT", 2, 2, "LIVING_AL1")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU3", "LIVING_AL1", "Tvariable"])
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
    elif name == duplicated_name("Marital_Language_CT", 3):
        u0 = value_calculate(path, "Marital_Language_CT", 3, 1, "TOTAL_NU5")
        u1 = value_calculate(path, "Marital_Language_CT", 3, 2, "MOVABLE_DW")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU5", "MOVABLE_DW", "Tvariable"])
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

# Define variable calculate function using variables of path, fieldName
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
                    if name == "Income_DA_":
                        duplicate(name, fc, outputFolder, 7)
                    elif name == "Marital_Language_DA":
                        duplicate(name, fc, outputFolder, 4)

                    elif name == "Income_Labour_Immigration_CT":
                        duplicate(name, fc, outputFolder, 10)
                    elif name == "Marital_Language_CT":
                        duplicate(name, fc, outputFolder, 4)

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
        if name == duplicated_name("Income_DA_", 0):
            # Define keepField & newName parameters for delete_rename function
            keepFields = ["FID", "Shape", "DAUID", "CDUID", "CDNAME", "CDTYPE", "CSDUID", "CSDNAME", "CSDTYPE", "CCSUID", "CCSNAME", "ERUID", "ERNAME", "CMAPUID", "CMAUID", "CMANAME", "CMATYPE", "SACCODE", "SACTYPE", "CTUID", "CTNAME", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "AVERAGE_HO", "POPULATI_1", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!AVERAGE_HO!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Household_Average_Income_Income_2011_DA.shp"
        elif name == duplicated_name("Income_DA_", 1):
            keepFields = ["FID", "Shape", "DAUID", "CDUID", "CDNAME", "CDTYPE", "CSDUID", "CSDNAME", "CSDTYPE", "CCSUID", "CCSNAME", "ERUID", "ERNAME", "CMAPUID", "CMAUID", "CMANAME", "CMATYPE", "SACCODE", "SACTYPE", "CTUID", "CTNAME", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "RENTER", "TOTAL_NU4", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!RENTER! / !TOTAL_NU4!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Rental_Income_2011_DA.shp"
        elif name == duplicated_name("Income_DA_", 2):
            keepFields = ["FID", "Shape", "DAUID", "CDUID", "CDNAME", "CDTYPE", "CSDUID", "CSDNAME", "CSDTYPE", "CCSUID", "CCSNAME", "ERUID", "ERNAME", "CMAPUID", "CMAUID", "CMANAME", "CMATYPE", "SACCODE", "SACTYPE", "CTUID", "CTNAME", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "PREVALENCE", "POPULATI_1", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!PREVALENCE!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Low_Income_Income_2011_DA.shp"
        elif name == duplicated_name("Income_DA_", 3):
            keepFields = ["FID", "Shape", "DAUID", "CDUID", "CDNAME", "CDTYPE", "CSDUID", "CSDNAME", "CSDTYPE", "CCSUID", "CCSNAME", "ERUID", "ERNAME", "CMAPUID", "CMAUID", "CMANAME", "CMATYPE", "SACCODE", "SACTYPE", "CTUID", "CTNAME", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "GOVERNMENT", "POPULATI_1", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!GOVERNMENT!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Government_Transfers_Income_2011_DA.shp"
        elif name == duplicated_name("Income_DA_", 4):
            keepFields = ["FID", "Shape", "DAUID", "CDUID", "CDNAME", "CDTYPE", "CSDUID", "CSDNAME", "CSDTYPE", "CCSUID", "CCSNAME", "ERUID", "ERNAME", "CMAPUID", "CMAUID", "CMANAME", "CMATYPE", "SACCODE", "SACTYPE", "CTUID", "CTNAME", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "AVERAGE_VA", "POPULATI_1", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!AVERAGE_VA!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Average_Value_Income_2011_DA.shp"
        elif name == duplicated_name("Income_DA_", 5):
            keepFields = ["FID", "Shape", "DAUID", "CDUID", "CDNAME", "CDTYPE", "CSDUID", "CSDNAME", "CSDTYPE", "CCSUID", "CCSNAME", "ERUID", "ERNAME", "CMAPUID", "CMAUID", "CMANAME", "CMATYPE", "SACCODE", "SACTYPE", "CTUID", "CTNAME", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "MAJOR_REPA", "TOTAL_NUMB", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!MAJOR_REPA! / !TOTAL_NUMB!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Major_Repair_Income_2011_DA.shp"
        elif name == duplicated_name("Income_DA_", 6):
            keepFields = ["FID", "Shape", "DAUID", "CDUID", "CDNAME", "CDTYPE", "CSDUID", "CSDNAME", "CSDTYPE", "CCSUID", "CCSNAME", "ERUID", "ERNAME", "CMAPUID", "CMAUID", "CMANAME", "CMATYPE", "SACCODE", "SACTYPE", "CTUID", "CTNAME", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "1960_OR_BE", "TOTAL_NUM1", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!1960_OR_BE! / !TOTAL_NUM1!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_1960Constructions_Income_2011_DA.shp"
        elif name == duplicated_name("Marital_Language_DA", 0):
            keepFields = ["FID", "Shape", "DAUID", "CDUID", "CDNAME", "CDTYPE", "CSDUID", "CSDNAME", "CSDTYPE", "CCSUID", "CCSNAME", "ERUID", "ERNAME", "CMAPUID", "CMAUID", "CMANAME", "CMATYPE", "SACCODE", "SACTYPE", "CTUID", "CTNAME", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "NEITHER_EN", "KNOWLEDGE_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!NEITHER_EN! / !KNOWLEDGE_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Neither_Language_Language_2011_DA.shp"
        elif name == duplicated_name("Marital_Language_DA", 1):
            keepFields = ["FID", "Shape", "DAUID", "CDUID", "CDNAME", "CDTYPE", "CSDUID", "CSDNAME", "CSDTYPE", "CCSUID", "CCSNAME", "ERUID", "ERNAME", "CMAPUID", "CMAUID", "CMANAME", "CMATYPE", "SACCODE", "SACTYPE", "CTUID", "CTNAME", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "TOTAL_LONE", "TOTAL_NUMB", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!TOTAL_LONE! / !TOTAL_NUMB!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Lone_Parent_marital_2011_DA.shp"
        elif name == duplicated_name("Marital_Language_DA", 2):
            keepFields = ["FID", "Shape", "DAUID", "CDUID", "CDNAME", "CDTYPE", "CSDUID", "CSDNAME", "CSDTYPE", "CCSUID", "CCSNAME", "ERUID", "ERNAME", "CMAPUID", "CMAUID", "CMANAME", "CMATYPE", "SACCODE", "SACTYPE", "CTUID", "CTNAME", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "LIVING_AL1", "TOTAL_NU3", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!LIVING_AL1! / !TOTAL_NU3!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Living_Alone_Marital_2011_DA.shp"
        elif name == duplicated_name("Marital_Language_DA", 3):
            keepFields = ["FID", "Shape", "DAUID", "CDUID", "CDNAME", "CDTYPE", "CSDUID", "CSDNAME", "CSDTYPE", "CCSUID", "CCSNAME", "ERUID", "ERNAME", "CMAPUID", "CMAUID", "CMANAME", "CMATYPE", "SACCODE", "SACTYPE", "CTUID", "CTNAME", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "MOVABLE_DW", "TOTAL_NU5", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!MOVABLE_DW! / !TOTAL_NU5!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Mobile_Marital_2011_DA.shp"

        elif name == duplicated_name("Income_Labour_Immigration_CT", 0):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "AVERAGE_HO", "POPULATION", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!AVERAGE_HO!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Household_Average_Income_Income_2011_CT.shp"
        elif name == duplicated_name("Income_Labour_Immigration_CT", 1):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "UNEMPLOYED", "IN_THE_LAB", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!UNEMPLOYED! / !IN_THE_LAB!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Labour_Unemployment_Education_2011_CT.shp"
        elif name == duplicated_name("Income_Labour_Immigration_CT", 2):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "RENTER", "TOTAL_NU4", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!RENTER! / !TOTAL_NU4!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Rental_Income_2011_CT.shp"
        elif name == duplicated_name("Income_Labour_Immigration_CT", 3):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "PREVALENCE", "POPULATION", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!PREVALENCE!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Low_Income_Income_2011_CT.shp"
        elif name == duplicated_name("Income_Labour_Immigration_CT", 4):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "GOVERNMENT", "POPULATION", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!GOVERNMENT!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Government_Transfers_Income_2011_CT.shp"
        elif name == duplicated_name("Income_Labour_Immigration_CT", 5):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "AVERAGE_VA", "POPULATION", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!AVERAGE_VA!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Average_Value_Income_2011_CT.shp"
        elif name == duplicated_name("Income_Labour_Immigration_CT", 6):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "MAJOR_REPA", "TOTAL_NUMB", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!MAJOR_REPA! / !TOTAL_NUMB!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Major_Repair_Income_2011_CT.shp"
        elif name == duplicated_name("Income_Labour_Immigration_CT", 7):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "1960_OR_BE", "TOTAL_NUM1", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!1960_OR_BE! / !TOTAL_NUM1!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_1960Constructions_Income_2011_CT.shp"
        elif name == duplicated_name("Income_Labour_Immigration_CT", 8):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "2006_TO_20", "TOTAL_POP1", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!2006_TO_20! / !TOTAL_POP1!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Immigration_Immigration_2011_CT.shp"
        elif name == duplicated_name("Income_Labour_Immigration_CT", 9):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "CANADIAN_C", "TOTAL_PO_1", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!CANADIAN_C! / !TOTAL_PO_1!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Canadian_Citizen_Immigration_2011_CT.shp"
        elif name == duplicated_name("Marital_Language_CT", 0):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "NEITHER_EN", "KNOWLEDGE_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!NEITHER_EN! / !KNOWLEDGE_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Neither_Language_Language_2011_CT.shp"
        elif name == duplicated_name("Marital_Language_CT", 1):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "TOTAL_LONE", "TOTAL_NUMB", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!TOTAL_LONE! / !TOTAL_NUMB!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Lone_Parent_Marital_2011_CT.shp"
        elif name == duplicated_name("Marital_Language_CT", 2):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "LIVING_AL1", "TOTAL_NU3", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!LIVING_AL1! / !TOTAL_NU3!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Living_Alone_Marital_2011_CT.shp"
        elif name == duplicated_name("Marital_Language_CT", 3):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "MOVABLE_DW", "TOTAL_NU5", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!MOVABLE_DW! / !TOTAL_NU5!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Mobile_Marital_2011_CT.shp"
        else:
            continue

        # Import delete_rename function only once instead execute in every lower tier if condition
        data_process(path, keepFields, expression0, expression1, newName)

if __name__ == '__main__':
    fcs_in_workspace(inputFolder)
