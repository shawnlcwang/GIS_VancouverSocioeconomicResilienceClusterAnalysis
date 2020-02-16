
# Import system modules
import arcpy
import os
import sys

arcpy.env.overwriteOutput = True

# Read the parameter values
# inputFolder = arcpy.GetParameterAsText(0)
# outputFolder = arcpy.GetParameterAsText(1)
inputFolder = "D:\\MSc Thesis\\CCAR Database\\2006\\Resilience_2006_Input"
outputFolder = "D:\\MSc Thesis\\CCAR Database\\2006\\Resilience_2006_Output"

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
    # factor = u / sd
    # return "!%s! * %f" % (fieldName, factor)

# Define Tvariable zero_data identification process with variable of path
def Tvariable_identify(path):
    # Automate add transformation variable field
    fieldName0 = arcpy.ValidateFieldName("Tvariable")
    arcpy.AddField_management(path, fieldName0, "TEXT", "", "", 50)
    # Automate field calculator for normalized variable
    arcpy.CalculateField_management(path, fieldName0, '""', "PYTHON_9.3")

# Define Tvariable process function using variables of path
def Tvariable_process(path):
    name = arcpy.Describe(path).name.strip(".shp")
    if name == duplicated_name("Labour_Occupations_Education_DA", 0):
        Tvariable_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["IN_THE_LAB", "Tvariable"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                row[0] = -9999
                row[1] = "Zero_Data"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Language_Immigration_Citizenship_DA", 0):
        Tvariable_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["POPULATIO1", "Tvariable"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                row[0] = -9999
                row[1] = "Zero_Data"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Language_Immigration_Citizenship_DA", 1):
        Tvariable_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_PO2", "Tvariable"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                row[0] = -9999
                row[1] = "Zero_Data"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Language_Immigration_Citizenship_DA", 2):
        Tvariable_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_PO5", "Tvariable"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                row[0] = -9999
                row[1] = "Zero_Data"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Martial_Families_Households_DA", 0):
        Tvariable_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU6", "Tvariable"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                row[0] = -9999
                row[1] = "Zero_Data"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Martial_Families_Households_DA", 1):
        Tvariable_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NUMB", "Tvariable"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                row[0] = -9999
                row[1] = "Zero_Data"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Martial_Families_Households_DA", 2):
        Tvariable_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU4", "Tvariable"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                row[0] = -9999
                row[1] = "Zero_Data"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Martial_Families_Households_DA", 3):
        Tvariable_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU7", "Tvariable"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                row[0] = -9999
                row[1] = "Zero_Data"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Martial_Families_Households_DA", 4):
        Tvariable_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU9", "Tvariable"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                row[0] = -9999
                row[1] = "Zero_Data"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Martial_Families_Households_DA", 5):
        Tvariable_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU8", "Tvariable"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                row[0] = -9999
                row[1] = "Zero_Data"
                cursor.updateRow(row)
        del row
        del cursor

    elif name == duplicated_name("Labour_Occupation_Education_CT", 0):
        Tvariable_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["IN_THE_LAB", "Tvariable"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                row[0] = -9999
                row[1] = "Zero_Data"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Language_Immigration_Citizenship_CT", 0):
        Tvariable_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["POPULATIO1", "Tvariable"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                row[0] = -9999
                row[1] = "Zero_Data"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Language_Immigration_Citizenship_CT", 1):
        Tvariable_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_PO2", "Tvariable"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                row[0] = -9999
                row[1] = "Zero_Data"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Language_Immigration_Citizenship_CT", 2):
        Tvariable_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_PO5", "Tvariable"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                row[0] = -9999
                row[1] = "Zero_Data"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Marital_Families_Households_CT", 0):
        Tvariable_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU4", "Tvariable"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                row[0] = -9999
                row[1] = "Zero_Data"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Marital_Families_Households_CT", 1):
        Tvariable_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NUMB", "Tvariable"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                row[0] = -9999
                row[1] = "Zero_Data"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Marital_Families_Households_CT", 2):
        Tvariable_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU4", "Tvariable"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                row[0] = -9999
                row[1] = "Zero_Data"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Marital_Families_Households_CT", 3):
        Tvariable_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU7", "Tvariable"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                row[0] = -9999
                row[1] = "Zero_Data"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Marital_Families_Households_CT", 4):
        Tvariable_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU9", "Tvariable"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                row[0] = -9999
                row[1] = "Zero_Data"
                cursor.updateRow(row)
        del row
        del cursor
    elif name == duplicated_name("Marital_Families_Households_CT", 5):
        Tvariable_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["TOTAL_NU8", "Tvariable"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                row[0] = -9999
                row[1] = "Zero_Data"
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
                    if name == "Income_Earnings_DA":
                        duplicate(name, fc, outputFolder, 4)
                    elif name == "Labour_Occupations_Education_DA":
                        duplicate(name, fc, outputFolder, 1)
                    elif name == "Language_Immigration_Citizenship_DA":
                        duplicate(name, fc, outputFolder, 3)
                    elif name == "Martial_Families_Households_DA":
                        duplicate(name, fc, outputFolder, 6)

                    elif name == "Income_Earnings_CT":
                        duplicate(name, fc, outputFolder, 4)
                    elif name == "Labour_Occupation_Education_CT":
                        duplicate(name, fc, outputFolder, 1)
                    elif name == "Language_Immigration_Citizenship_CT":
                        duplicate(name, fc, outputFolder, 3)
                    elif name == "Marital_Families_Households_CT":
                        duplicate(name, fc, outputFolder, 6)

    # Change environment workspace within the loop
    arcpy.env.workspace = outputFolder
    # List feature classes
    fclist = arcpy.ListFeatureClasses()
    # Loop for fc in fclist
    for fc in fclist:
        # Define path parameter for delete_rename function
        path = outputFolder + "\\" + fc
        name = arcpy.Describe(path).name.strip(".shp")
        Tvariable_process(path)
        arcpy.AddField_management(path, "normalize", "DOUBLE", "", "", 50)
        # Pre-declare variables of keepFields & newName with empty container for delete_rename function
        # Allow to let the lower tier block of different keepFields & newName codes
        # Fill in the upper tier keepFields & newName
        keepFields = []
        expression0 = ""
        expression1 = ""
        newName = ""

        # Import duplicated_name function to define names for if conditions
        if name == duplicated_name("Income_Earnings_DA", 0):
            # Define keepField & newName parameters for delete_rename function
            keepFields = ["FID", "Shape", "DAUID", "CSDUID", "CCSUID", "CDUID", "ERUID", "PRUID", "CTUID", "CMAUID", "OID_", "GEOGRAPHY", "AVERAGE_HO", "TFvariable", "Z_score"]
            expression0 = "!AVERAGE_HO!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Household_Average_Income_Income_2006_DA.shp"
        elif name == duplicated_name("Income_Earnings_DA", 1):
            keepFields = ["FID", "Shape", "DAUID", "CSDUID", "CCSUID", "CDUID", "ERUID", "PRUID", "CTUID", "CMAUID", "OID_", "GEOGRAPHY", "PREVALENCE", "TFvariable", "Z_score"]
            expression0 = "!PREVALENCE!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Low_Income_Income_2006_DA.shp"
        elif name == duplicated_name("Income_Earnings_DA", 2):
            keepFields = ["FID", "Shape", "DAUID", "CSDUID", "CCSUID", "CDUID", "ERUID", "PRUID", "CTUID", "CMAUID", "OID_", "GEOGRAPHY", "GOVERNMENT", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!GOVERNMENT!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Government_Transfers_Income_2006_DA.shp"
        elif name == duplicated_name("Income_Earnings_DA", 3):
            keepFields = ["FID", "Shape", "DAUID", "CSDUID", "CCSUID", "CDUID", "ERUID", "PRUID", "CTUID", "CMAUID", "OID_", "GEOGRAPHY", "AVERAGE_VA", "TFvariable", "Z_score"]
            expression0 = "!AVERAGE_VA!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Average_Value_Income_2006_DA.shp"
        elif name == duplicated_name("Labour_Occupations_Education_DA", 0):
            keepFields = ["FID", "Shape", "DAUID", "CSDUID", "CCSUID", "CDUID", "ERUID", "PRUID", "CTUID", "CMAUID", "OID_", "GEOGRAPHY", "UNEMPLOYED", "IN_THE_LAB", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!UNEMPLOYED! / !IN_THE_LAB!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Labour_Unemployment_Labour_2006_DA.shp"
        elif name == duplicated_name("Language_Immigration_Citizenship_DA", 0):
            keepFields = ["FID", "Shape", "DAUID", "CSDUID", "CCSUID", "CDUID", "ERUID", "PRUID", "CTUID", "CMAUID", "OID_", "GEOGRAPHY", "2001_TO_20", "POPULATIO1", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!2001_TO_20! / !POPULATIO1!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Immigration_Language_2006_DA.shp"
        elif name == duplicated_name("Language_Immigration_Citizenship_DA", 1):
            keepFields = ["FID", "Shape", "DAUID", "CSDUID", "CCSUID", "CDUID", "ERUID", "PRUID", "CTUID", "CMAUID", "OID_", "GEOGRAPHY", "NEITHER_EN", "TOTAL_PO2", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!NEITHER_EN! / !TOTAL_PO2!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Neither_Language_Language_2006_DA.shp"
        elif name == duplicated_name("Language_Immigration_Citizenship_DA", 2):
            keepFields = ["FID", "Shape", "DAUID", "CSDUID", "CCSUID", "CDUID", "ERUID", "PRUID", "CTUID", "CMAUID", "OID_", "GEOGRAPHY", "CANADIAN_C", "TOTAL_PO5", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!CANADIAN_C! / !TOTAL_PO5!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Canadian_Citizen_Language_2006_DA.shp"
        elif name == duplicated_name("Martial_Families_Households_DA", 0):
            keepFields = ["FID", "Shape", "DAUID", "CSDUID", "CCSUID", "CDUID", "ERUID", "PRUID", "CTUID", "CMAUID", "OID_", "GEOGRAPHY", "RENTED", "TOTAL_NU6", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!RENTED! / !TOTAL_NU6!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Rental_Marital_2006_DA.shp"
        elif name == duplicated_name("Martial_Families_Households_DA", 1):
            keepFields = ["FID", "Shape", "DAUID", "CSDUID", "CCSUID", "CDUID", "ERUID", "PRUID", "CTUID", "CMAUID", "OID_", "GEOGRAPHY", "TOTAL_LONE", "TOTAL_NUMB", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!TOTAL_LONE! / !TOTAL_NUMB!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Lone_Parent_Marital_2006_DA.shp"
        elif name == duplicated_name("Martial_Families_Households_DA", 2):
            keepFields = ["FID", "Shape", "DAUID", "CSDUID", "CCSUID", "CDUID", "ERUID", "PRUID", "CTUID", "CMAUID", "OID_", "GEOGRAPHY", "LIVING_AL1", "TOTAL_NU4", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!LIVING_AL1! / !TOTAL_NU4!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Living_Alone_Marital_2006_DA.shp"
        elif name == duplicated_name("Martial_Families_Households_DA", 3):
            keepFields = ["FID", "Shape", "DAUID", "CSDUID", "CCSUID", "CDUID", "ERUID", "PRUID", "CTUID", "CMAUID", "OID_", "GEOGRAPHY", "MAJOR_REPA", "TOTAL_NU7", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!MAJOR_REPA! / !TOTAL_NU7!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Major_Repair_Martial_2006_DA.shp"
        elif name == duplicated_name("Martial_Families_Households_DA", 4):
            keepFields = ["FID", "Shape", "DAUID", "CSDUID", "CCSUID", "CDUID", "ERUID", "PRUID", "CTUID", "CMAUID", "OID_", "GEOGRAPHY", "MOVABLE_DW", "TOTAL_NU9", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!MOVABLE_DW! / !TOTAL_NU9!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Mobile_Marital_2006_DA.shp"
        elif name == duplicated_name("Martial_Families_Households_DA", 5):
            keepFields = ["FID", "Shape", "DAUID", "CSDUID", "CCSUID", "CDUID", "ERUID", "PRUID", "CTUID", "CMAUID", "OID_", "GEOGRAPHY", "PERIOD_OF_", "PERIOD_OF1", "TOTAL_NU8", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "(!PERIOD_OF_! + !PERIOD_OF1!) / !TOTAL_NU8!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_1960Constructions_Marital_2006_DA.shp"

        elif name == duplicated_name("Income_Earnings_CT", 0):
            keepFields = ["FID", "Shape", "CTUID", "CMAUID", "PRUID", "CT_UID", "OID_", "GEOGRAPHY", "AVERAGE_HO", "TFvariable", "Z_score"]
            expression0 = "!AVERAGE_HO!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Household_Average_Income_Income_2006_CT.shp"
        elif name == duplicated_name("Income_Earnings_CT", 1):
            keepFields = ["FID", "Shape", "CTUID", "CMAUID", "PRUID", "CT_UID", "OID_", "GEOGRAPHY", "PREVALENCE", "TFvariable", "Z_score"]
            expression0 = "!PREVALENCE!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Low_Income_Income_2006_CT.shp"
        elif name == duplicated_name("Income_Earnings_CT", 2):
            keepFields = ["FID", "Shape", "CTUID", "CMAUID", "PRUID", "CT_UID", "OID_", "GEOGRAPHY", "GOVERNMENT", "TFvariable", "Z_score"]
            expression0 = "!GOVERNMENT!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Government_Transfers_Income_2006_CT.shp"
        elif name == duplicated_name("Income_Earnings_CT", 3):
            keepFields = ["FID", "Shape", "CTUID", "CMAUID", "PRUID", "CT_UID", "OID_", "GEOGRAPHY", "AVERAGE_VA", "TFvariable", "Z_score"]
            expression0 = "!AVERAGE_VA!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Average_Value_Income_2006_CT.shp"
        elif name == duplicated_name("Labour_Occupation_Education_CT", 0):
            keepFields = ["FID", "Shape", "CTUID", "CMAUID", "PRUID", "CT_UID", "OID_", "GEOGRAPHY", "UNEMPLOYED", "IN_THE_LAB", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!UNEMPLOYED! / !IN_THE_LAB!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Labour_Unemployment_Labour_2006_CT.shp"
        elif name == duplicated_name("Language_Immigration_Citizenship_CT", 0):
            keepFields = ["FID", "Shape", "CTUID", "CMAUID", "PRUID", "CT_UID", "OID_", "GEOGRAPHY", "2001_TO_20", "POPULATIO1", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!2001_TO_20! / !POPULATIO1!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Immigration_Language_2006_CT.shp"
        elif name == duplicated_name("Language_Immigration_Citizenship_CT", 1):
            keepFields = ["FID", "Shape", "CTUID", "CMAUID", "PRUID", "CT_UID", "OID_", "GEOGRAPHY", "NEITHER_EN", "TOTAL_PO2", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!NEITHER_EN! / !TOTAL_PO2!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Neither_Language_Language_2006_CT.shp"
        elif name == duplicated_name("Language_Immigration_Citizenship_CT", 2):
            keepFields = ["FID", "Shape", "CTUID", "CMAUID", "PRUID", "CT_UID", "OID_", "GEOGRAPHY", "CANADIAN_C", "TOTAL_PO5", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!CANADIAN_C! / !TOTAL_PO5!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Canadian_Citizen_Language_2006_CT.shp"
        elif name == duplicated_name("Marital_Families_Households_CT", 0):
            keepFields = ["FID", "Shape", "CTUID", "CMAUID", "PRUID", "CT_UID", "OID_", "GEOGRAPHY", "RENTED", "TOTAL_NU4", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!RENTED! / !TOTAL_NU4!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Rental_Marital_2006_CT.shp"
        elif name == duplicated_name("Marital_Families_Households_CT", 1):
            keepFields = ["FID", "Shape", "CTUID", "CMAUID", "PRUID", "CT_UID", "OID_", "GEOGRAPHY", "TOTAL_LONE", "TOTAL_NUMB", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!TOTAL_LONE! / !TOTAL_NUMB!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Lone_Parent_Marital_2006_CT.shp"
        elif name == duplicated_name("Marital_Families_Households_CT", 2):
            keepFields = ["FID", "Shape", "CTUID", "CMAUID", "PRUID", "CT_UID", "OID_", "GEOGRAPHY", "LIVING_AL1", "TOTAL_NU4", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!LIVING_AL1! / !TOTAL_NU4!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Living_Alone_Marital_2006_CT.shp"
        elif name == duplicated_name("Marital_Families_Households_CT", 3):
            keepFields = ["FID", "Shape", "CTUID", "CMAUID", "PRUID", "CT_UID", "OID_", "GEOGRAPHY", "MAJOR_REPA", "TOTAL_NU7", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!MAJOR_REPA! / !TOTAL_NU7!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Major_Repair_Marital_2006_CT.shp"
        elif name == duplicated_name("Marital_Families_Households_CT", 4):
            keepFields = ["FID", "Shape", "CTUID", "CMAUID", "PRUID", "CT_UID", "OID_", "GEOGRAPHY", "MOVABLE_DW", "TOTAL_NU9", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!MOVABLE_DW! / !TOTAL_NU9!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Mobile_Marital_2006_CT.shp"
        elif name == duplicated_name("Marital_Families_Households_CT", 5):
            keepFields = ["FID", "Shape", "CTUID", "CMAUID", "PRUID", "CT_UID", "OID_", "GEOGRAPHY", "PERIOD_OF_", "PERIOD_OF1", "TOTAL_NU8", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "(PERIOD_OF_! + !PERIOD_OF1!)/ !TOTAL_NU8!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_1960Constructions_Marital_2006_CT.shp"
        else:
            continue

        # Import delete_rename function only once instead execute in every lower tier if condition
        data_process(path, keepFields, expression0, expression1, newName)

if __name__ == '__main__':
    fcs_in_workspace(inputFolder)
