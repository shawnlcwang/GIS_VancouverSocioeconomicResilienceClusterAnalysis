
# Import system modules
import arcpy
import os
import sys

arcpy.env.overwriteOutput = True

# Read the parameter values
# inputFolder = arcpy.GetParameterAsText(0)
# outputFolder = arcpy.GetParameterAsText(1)
inputFolder = "D:\\MSc Thesis\\CCAR Database\\1996\\Social_Resilience_1996_Input"
outputFolder = "D:\\MSc Thesis\\CCAR Database\\1996\\Social_Resilience_1996_Output"

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
def value_calculate(path, name, x, y, fieldName, fieldName_, expression):
    # Automate field calculator for fieldName
    fieldName1 = arcpy.ValidateFieldName(fieldName_)
    arcpy.AddField_management(path, fieldName1, "DOUBLE", "", "", 50)
    arcpy.CalculateField_management(path, fieldName1, expression, "PYTHON_9.3")
    # Create fcName variable by importing duplicate_name1 function
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
    # Automate field calculator for Tvariable
    fieldName0 = arcpy.ValidateFieldName("Tvariable")
    arcpy.AddField_management(path, fieldName0, "TEXT", "", "", 50)
    arcpy.CalculateField_management(path, fieldName0, '""', "PYTHON_9.3")
    name = arcpy.Describe(path).name.strip(".shp")
    if name == duplicated_name("Income_EA", 0):
        u0 = value_calculate(path, "Income_EA", 0, 1, "AVERAGE31", "AVERAGE31_", "!AVERAGE31!")
        cursor = arcpy.da.UpdateCursor(path, ["POPULATION", "AVERAGE31_", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
            if row[1] == -9999:
                row[1] = u0
                row[2] = "N/A"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Income_EA", 1):
        u0 = value_calculate(path, "Income_EA", 1, 1, "TOTAL_NU4", "TOTAL_NU4_", "!TOTAL_NU4!")
        u1 = value_calculate(path, "Income_EA", 1, 2, "RENTED", "RENTED_", "!RENTED!")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU4_", "RENTED_", "Tvariable"])
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
    elif name == duplicated_name("Income_EA", 2):
        u0 = value_calculate(path, "Income_EA", 2, 1, "AVERAGE_VA", "AVERAGE_V_", "!AVERAGE_VA!")
        cursor = arcpy.da.UpdateCursor(path, ["POPULATION", "AVERAGE_V_", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
            if row[1] == -9999:
                row[1] = u0
                row[2] = "N/A"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Income_EA", 3):
        u0 = value_calculate(path, "Income_EA", 3, 1, "TOTAL_NU4", "TOTAL_NU4_", "!TOTAL_NU4!")
        u1 = value_calculate(path, "Income_EA", 3, 2, "MAJOR_REPA", "MAJOR_REP_", "!MAJOR_REPA!")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU4_", "MAJOR_REP_", "Tvariable"])
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
    elif name == duplicated_name("Income_EA", 4):
        u0 = value_calculate(path, "Income_EA", 4, 1, "TOTAL_NU4", "TOTAL_NU4_", "!TOTAL_NU4!")
        u1 = value_calculate(path, "Income_EA", 4, 2, "PERIOD_OF_", "PERIOD_O_", "!PERIOD_OF_!")
        u2 = value_calculate(path, "Income_EA", 4, 3, "PERIOD_OF1", "PERIOD_O1_", "!PERIOD_OF1!")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU4_", "PERIOD_O_", "PERIOD_O1_", "Tvariable"])
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
    elif name == duplicated_name("Labour_EA", 0):
        u0 = value_calculate(path, "Labour_EA", 0, 1, "IN_THE_LAB", "IN_THE_LA_", "!IN_THE_LAB!")
        u1 = value_calculate(path, "Labour_EA", 0, 2, "UNEMPLOYED", "UNEMPLOYE_", "!UNEMPLOYED!")
        cursor = arcpy.da.UpdateCursor(path, ["IN_THE_LA_", "UNEMPLOYE_", "Tvariable"])
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
    elif name == duplicated_name("Language_EA", 0):
        u0 = value_calculate(path, "Language_EA", 0, 2, "TOTAL_POPU", "TOTAL_POP_", "!TOTAL_POPU!")
        u1 = value_calculate(path, "Language_EA", 0, 1, "1991_1996_", "_1991_1996", "!1991_1996_!")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_POP_", "_1991_1996", "Tvariable"])
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
    elif name == duplicated_name("Language_EA", 1):
        u0 = value_calculate(path, "Language_EA", 1, 1, "TOTAL_PO3", "TOTAL_PO3_", "!TOTAL_PO3!")
        u1 = value_calculate(path, "Language_EA", 1, 2, "NEITHER_EN", "NEITHER_E_", "!NEITHER_EN!")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_PO3_", "NEITHER_E_", "Tvariable"])
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
    elif name == duplicated_name("Language_EA", 2):
        u0 = value_calculate(path, "Language_EA", 2, 1, "TOTAL_POPU", "TOTAL_POP_", "!TOTAL_POPU!")
        u1 = value_calculate(path, "Language_EA", 2, 2, "CANADIAN_C", "CANADIAN__", "!CANADIAN_C!")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_POP_", "CANADIAN__", "Tvariable"])
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
    elif name == duplicated_name("Marital_EA", 0):
        u0 = value_calculate(path, "Marital_EA", 0, 1, "TOTAL_NUMB", "TOTAL_NUM_", "!TOTAL_NUMB!")
        u1 = value_calculate(path, "Marital_EA", 0, 2, "TOTAL_LONE", "TOTAL_LON_", "!TOTAL_LONE!")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NUM_", "TOTAL_LON_", "Tvariable"])
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
    elif name == duplicated_name("Marital_EA", 1):
        u0 = value_calculate(path, "Marital_EA", 1, 1, "TOTAL_NU3", "TOTAL_NU3_", "!TOTAL_NU3!")
        u1 = value_calculate(path, "Marital_EA", 1, 2, "LIVING_AL1", "LIVING_AL_", "!LIVING_AL1!")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU3_", "LIVING_AL_", "Tvariable"])
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
    elif name == duplicated_name("Marital_EA", 2):
        u0 = value_calculate(path, "Marital_EA", 2, 1, "TOTAL_NU4", "TOTAL_NU4_", "!TOTAL_NU4!")
        u1 = value_calculate(path, "Marital_EA", 2, 2, "MOVABLE_DW", "MOVABLE_D_", "!MOVABLE_DW!")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU4_", "MOVABLE_D_", "Tvariable"])
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

    if name == duplicated_name("Income_CT", 0):
        u0 = value_calculate(path, "Income_CT", 0, 1, "AVERAGE_HO", "AVERAGE_H_", "!AVERAGE_HO!")
        cursor = arcpy.da.UpdateCursor(path, ["POPULATIO1", "AVERAGE_H_", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
            if row[1] == -9999:
                row[1] = u0
                row[2] = "N/A"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Income_CT", 1):
        u0 = value_calculate(path, "Income_CT", 1, 1, "TOTAL_NU4", "TOTAL_NU4_", "!TOTAL_NU4!")
        u1 = value_calculate(path, "Income_CT", 1, 2, "RENTED", "RENTED_", "!RENTED!")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU4_", "RENTED_", "Tvariable"])
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
    elif name == duplicated_name("Income_CT", 2):
        u0 = value_calculate(path, "Income_CT", 2, 1, "INCIDENC2", "INCIDENC2_", "!INCIDENC2!")
        cursor = arcpy.da.UpdateCursor(path, ["POPULATIO1", "INCIDENC2_", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
            if row[1] == -9999:
                row[1] = u0
                row[2] = "N/A"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Income_CT", 3):
        u0 = value_calculate(path, "Income_CT", 3, 1, "GOVERNMENT", "GOVERNMEN_", "!GOVERNMENT!")
        cursor = arcpy.da.UpdateCursor(path, ["POPULATIO1", "GOVERNMEN_", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
            if row[1] == -9999:
                row[1] = u0
                row[2] = "N/A"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Income_CT", 4):
        u0 = value_calculate(path, "Income_CT", 4, 1, "AVERAGE_VA", "AVERAGE_V_", "!AVERAGE_VA!")
        cursor = arcpy.da.UpdateCursor(path, ["POPULATIO1", "AVERAGE_V_", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
            if row[1] == -9999:
                row[1] = u0
                row[2] = "N/A"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Income_CT", 5):
        u0 = value_calculate(path, "Income_CT", 5, 1, "TOTAL_NU4", "TOTAL_NU4_", "!TOTAL_NU4!")
        u1 = value_calculate(path, "Income_CT", 5, 2, "MAJOR_REPA", "MAJOR_REP_", "!MAJOR_REPA!")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU4_", "MAJOR_REP_", "Tvariable"])
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
    elif name == duplicated_name("Income_CT", 6):
        u0 = value_calculate(path, "Income_CT", 6, 1, "TOTAL_NU4", "TOTAL_NU4_", "!TOTAL_NU4!")
        u1 = value_calculate(path, "Income_CT", 6, 2, "PERIOD_OF_", "PERIOD_O_", "!PERIOD_OF_!")
        u2 = value_calculate(path, "Income_CT", 6, 3, "PERIOD_OF1", "PERIOD_O1_", "!PERIOD_OF1!")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU4_", "PERIOD_O_", "PERIOD_O1_", "Tvariable"])
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
    elif name == duplicated_name("Labour_CT", 0):
        u0 = value_calculate(path, "Labour_CT", 0, 1, "IN_THE_LAB", "IN_THE_LA_", "!IN_THE_LAB!")
        u1 = value_calculate(path, "Labour_CT", 0, 2, "UNEMPLOYED", "UNEMPLOYE_", "!UNEMPLOYED!")
        cursor = arcpy.da.UpdateCursor(path, ["IN_THE_LA_", "UNEMPLOYE_", "Tvariable"])
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
    elif name == duplicated_name("Language_CT", 0):
        u0 = value_calculate(path, "Language_CT", 0, 2, "TOTAL_POPU", "TOTAL_POP_", "!TOTAL_POPU!")
        u1 = value_calculate(path, "Language_CT", 0, 1, "1991_1996_", "_1991_1996", "!1991_1996_!")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_POP_", "_1991_1996", "Tvariable"])
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
    elif name == duplicated_name("Language_CT", 1):
        u0 = value_calculate(path, "Language_CT", 1, 1, "TOTAL_PO3", "TOTAL_PO3_", "!TOTAL_PO3!")
        u1 = value_calculate(path, "Language_CT", 1, 2, "NEITHER_EN", "NEITHER_E_", "!NEITHER_EN!")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_PO3_", "NEITHER_E_", "Tvariable"])
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
    elif name == duplicated_name("Language_CT", 2):
        u0 = value_calculate(path, "Language_CT", 2, 1, "TOTAL_POPU", "TOTAL_POP_", "!TOTAL_POPU!")
        u1 = value_calculate(path, "Language_CT", 2, 2, "CANADIAN_C", "CANADIAN__", "!CANADIAN_C!")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_POP_", "CANADIAN__", "Tvariable"])
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
    elif name == duplicated_name("Marital_CT", 0):
        u0 = value_calculate(path, "Marital_CT", 0, 1, "TOTAL_NUMB", "TOTAL_NUM_", "!TOTAL_NUMB!")
        u1 = value_calculate(path, "Marital_CT", 0, 2, "TOTAL_LONE", "TOTAL_LON_", "!TOTAL_LONE!")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NUM_", "TOTAL_LON_", "Tvariable"])
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
    elif name == duplicated_name("Marital_CT", 1):
        u0 = value_calculate(path, "Marital_CT", 1, 1, "TOTAL_NU3", "TOTAL_NU3_", "!TOTAL_NU3!")
        u1 = value_calculate(path, "Marital_CT", 1, 2, "LIVING_AL1", "LIVING_AL_", "!LIVING_AL1!")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU3_", "LIVING_AL_", "Tvariable"])
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
    elif name == duplicated_name("Marital_CT", 2):
        u0 = value_calculate(path, "Marital_CT", 2, 1, "TOTAL_NU4", "TOTAL_NU4_", "!TOTAL_NU4!")
        u1 = value_calculate(path, "Marital_CT", 2, 2, "MOVABLE_DW", "MOVABLE_D_", "!MOVABLE_DW!")
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU4_", "MOVABLE_D_", "Tvariable"])
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
                    if name == "Income_EA":
                        duplicate(name, fc, outputFolder, 5)
                    elif name == "Labour_EA":
                        duplicate(name, fc, outputFolder, 1)
                    elif name == "Language_EA":
                        duplicate(name, fc, outputFolder, 3)
                    elif name == "Marital_EA":
                        duplicate(name, fc, outputFolder, 3)

                    elif name == "Income_CT":
                        duplicate(name, fc, outputFolder, 7)
                    elif name == "Labour_CT":
                        duplicate(name, fc, outputFolder, 1)
                    elif name == "Language_CT":
                        duplicate(name, fc, outputFolder, 3)
                    elif name == "Marital_CT":
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
        if name == duplicated_name("Income_EA", 0):
            # Define keepField & newName parameters for delete_rename function
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "AVERAGE31", "AVERAGE31_", "POPULATION", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!AVERAGE31_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Household_Average_Income_Income_1996_EA.shp"
        elif name == duplicated_name("Income_EA", 1):
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "RENTED", "RENTED_", "TOTAL_NU4", "TOTAL_NU4_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!RENTED_! / !TOTAL_NU4_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Rental_Income_1996_EA.shp"
        elif name == duplicated_name("Income_EA", 2):
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "AVERAGE_VA", "AVERAGE_V_", "POPULATION", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!AVERAGE_V_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Average_Value_Income_1996_EA.shp"
        elif name == duplicated_name("Income_EA", 3):
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "MAJOR_REPA", "MAJOR_REP_", "TOTAL_NU4", "TOTAL_NU4_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!MAJOR_REP_! / !TOTAL_NU4_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Major_Repair_Income_1996_EA.shp"
        elif name == duplicated_name("Income_EA", 4):
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "PERIOD_OF_", "PERIOD_O_", "PERIOD_OF1", "PERIOD_O1_", "TOTAL_NU4", "TOTAL_NU4_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "(!PERIOD_O_! + !PERIOD_O1_!)/ !TOTAL_NU4_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_1960Constructions_Income_1991_EA.shp"
        elif name == duplicated_name("Labour_EA", 0):
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "UNEMPLOYED", "UNEMPLOYE_", "IN_THE_LAB", "IN_THE_LA_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!UNEMPLOYE_! / !IN_THE_LA_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Labour_Unemployment_Labour_1996_EA.shp"
        elif name == duplicated_name("Language_EA", 0):
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "1991_1996_", "_1991_1996", "TOTAL_POPU", "TOTAL_POP_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!_1991_1996! / !TOTAL_POP_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Immigration_Language_1996_EA.shp"
        elif name == duplicated_name("Language_EA", 1):
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "NEITHER_EN", "NEITHER_E_", "TOTAL_PO3", "TOTAL_PO3_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!NEITHER_E_! / !TOTAL_PO3_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Neither_Language_Language_1996_EA.shp"
        elif name == duplicated_name("Language_EA", 2):
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "CANADIAN_C", "CANADIAN__", "TOTAL_POPU", "TOTAL_POP_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!CANADIAN__! / !TOTAL_POP_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Canadian_Citizen_Language_1996_EA.shp"
        elif name == duplicated_name("Marital_EA", 0):
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "TOTAL_LONE", "TOTAL_LON_", "TOTAL_NUMB", "TOTAL_NUM_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!TOTAL_LON_! / !TOTAL_NUM_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Lone_Parent_Marital_1996_EA.shp"
        elif name == duplicated_name("Marital_EA", 1):
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "LIVING_AL1", "LIVING_AL_", "TOTAL_NU3", "TOTAL_NU3_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!LIVING_AL_! / !TOTAL_NU3_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Living_Alone_Marital_1996_EA.shp"
        elif name == duplicated_name("Marital_EA", 2):
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "MOVABLE_DW", "MOVABLE_D_", "TOTAL_NU4", "TOTAL_NU4_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!MOVABLE_D_! / !TOTAL_NU4_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Mobile_Marital_1996_EA.shp"

        elif name == duplicated_name("Income_CT", 0):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "AVERAGE_HO", "AVERAGE_H_", "POPULATIO1", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!AVERAGE_H_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Household_Average_Income_Income_1996_CT.shp"
        elif name == duplicated_name("Income_CT", 1):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "RENTED", "RENTED_", "TOTAL_NU4", "TOTAL_NU4_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!RENTED_! / !TOTAL_NU4_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Rental_Income_1996_CT.shp"
        elif name == duplicated_name("Income_CT", 2):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "INCIDENC2", "INCIDENC2_", "POPULATIO1", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!INCIDENC2_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Low_Income_Income_1996_CT.shp"
        elif name == duplicated_name("Income_CT", 3):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "GOVERNMENT", "GOVERNMEN_", "POPULATIO1", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!GOVERNMEN_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Government_Transfers_Income_1996_CT.shp"
        elif name == duplicated_name("Income_CT", 4):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "AVERAGE_VA", "AVERAGE_V_", "POPULATIO1", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!AVERAGE_V_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Average_Value_Income_1996_CT.shp"
        elif name == duplicated_name("Income_CT", 5):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "MAJOR_REPA", "MAJOR_REP_", "TOTAL_NU4", "TOTAL_NU4_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!MAJOR_REP_! / !TOTAL_NU4_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Major_Repair_Income_1996_CT.shp"
        elif name == duplicated_name("Income_CT", 6):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "PERIOD_OF_", "PERIOD_O_", "PERIOD_OF1", "PERIOD_O1_", "TOTAL_NU4", "TOTAL_NU4_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "(!PERIOD_O_! + !PERIOD_O1_!) / !TOTAL_NU4_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_1960Constructions_Income_1996_CT.shp"
        elif name == duplicated_name("Labour_CT", 0):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "UNEMPLOYED", "UNEMPLOYE_", "IN_THE_LAB", "IN_THE_LA_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!UNEMPLOYE_! / !IN_THE_LA_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Labour_Unemployment_Labour_1996_CT.shp"
        elif name == duplicated_name("Language_CT", 0):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "1991_1996_", "_1991_1996", "TOTAL_POPU", "TOTAL_POP_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!_1991_1996! / !TOTAL_POP_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Immigration_Language_1996_CT.shp"
        elif name == duplicated_name("Language_CT", 1):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "NEITHER_EN", "NEITHER_E_", "TOTAL_PO3", "TOTAL_PO3_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!NEITHER_E_! / !TOTAL_PO3_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Neither_Language_Language_1996_CT.shp"
        elif name == duplicated_name("Language_CT", 2):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "CANADIAN_C", "CANADIAN__", "TOTAL_POPU", "TOTAL_POP_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!CANADIAN__! / !TOTAL_POP_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Canadian_Citizen_Language_1996_CT.shp"
        elif name == duplicated_name("Marital_CT", 0):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "TOTAL_LONE", "TOTAL_LON_", "TOTAL_NUMB", "TOTAL_NUM_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!TOTAL_LON_! / !TOTAL_NUM_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Lone_Parent_Marital_1996_CT.shp"
        elif name == duplicated_name("Marital_CT", 1):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "LIVING_AL1", "LIVING_AL_", "TOTAL_NU3", "TOTAL_NU3_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!LIVING_AL_! / !TOTAL_NU3_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Living_Alone_Marital_1996_CT.shp"
        elif name == duplicated_name("Marital_CT", 2):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "MOVABLE_DW", "MOVABLE_D_", "TOTAL_NU4", "TOTAL_NU4_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!MOVABLE_D_! / !TOTAL_NU4_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Mobile_Marital_1996_CT.shp"
        else:
            continue

        # Import delete_rename function only once instead execute in every lower tier if condition
        data_process(path, keepFields, expression0, expression1, newName)

if __name__ == '__main__':
    fcs_in_workspace(inputFolder)
