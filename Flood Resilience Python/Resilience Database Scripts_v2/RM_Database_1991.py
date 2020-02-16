
# Import system modules
import arcpy
import os
import sys

arcpy.env.overwriteOutput = True

# Read the parameter values
# inputFolder = arcpy.GetParameterAsText(0)
# outputFolder = arcpy.GetParameterAsText(1)
inputFolder = "D:\\MSc Thesis\\CCAR Database\\1991\\Resilience_1991_Input"
outputFolder = "D:\\MSc Thesis\\CCAR Database\\1991\\Resilience_1991_Output2"

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

# Define null value process function using variables of path
def null_process(path):
    name = arcpy.Describe(path).name.strip(".shp")
    if name == duplicated_name("Dwelling_households_EA", 0):
        null_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["COL6"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Dwelling_households_EA", 1):
        null_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["COL6"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Dwelling_households_B_EA", 1):
        null_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["COL27"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Dwelling_households_B_EA", 2):
        null_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["COL6"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Dwelling_households_B_EA", 3):
        null_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["COL6"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Families_EA", 0):
        null_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["COL47"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Labor_activity_B_EA", 0):
        null_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["COL7"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Language_B_EA_", 0):
        null_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["COL6"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Reli_ethnic_ori_citzn_immi_B_EA_", 0):
        null_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["COL6"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Reli_ethnic_ori_citzn_immi_B_EA_", 1):
        null_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["COL6"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                cursor.deleteRow()
        del row
        del cursor

    elif name == duplicated_name("Citizen_Immigration_Migration_CT", 0):
        null_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["COL4"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Citizen_Immigration_Migration_CT", 1):
        null_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["COL7"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Labour_Employment_Occupation_CT", 0):
        null_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["COL43"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Language_CT", 0):
        null_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["COL4"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Marital_Families_Households_Dwellings_CT", 0):
        null_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["COL47"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Marital_Families_Households_Dwellings_CT", 2):
        null_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["COL70"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Marital_Families_Households_Dwellings_CT", 3):
        null_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["COL111"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Marital_Families_Households_Dwellings_CT", 4):
        null_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["COL138"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Marital_Families_Households_Dwellings_CT", 5):
        null_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["COL47"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Marital_Families_Households_Dwellings_CT", 6):
        null_identify(path)
        cursor = arcpy.da.UpdateCursor(path, ["COL138"])
        for row in cursor:
            if row[0] == 0 or row[0] is None:
                cursor.deleteRow()
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
                    if name == "Dwelling_households_EA":
                        duplicate(name, fc, outputFolder, 2)
                    elif name == "Dwelling_households_B_EA":
                        duplicate(name, fc, outputFolder, 4)
                    elif name == "Families_EA":
                        duplicate(name, fc, outputFolder, 1)
                    elif name == "Income_EA":
                        duplicate(name, fc, outputFolder, 3)
                    elif name == "Labor_activity_B_EA":
                        duplicate(name, fc, outputFolder, 1)
                    elif name == "Language_B_EA_":
                        duplicate(name, fc, outputFolder, 1)
                    elif name == "Reli_ethnic_ori_citzn_immi_B_EA_":
                        duplicate(name, fc, outputFolder, 2)

                    elif name == "Citizen_Immigration_Migration_CT":
                        duplicate(name, fc, outputFolder, 2)
                    elif name == "Education_income_CT":
                        duplicate(name, fc, outputFolder, 3)
                    elif name == "Labour_Employment_Occupation_CT":
                        duplicate(name, fc, outputFolder, 1)
                    elif name == "Language_CT":
                        duplicate(name, fc, outputFolder, 1)
                    elif name == "Marital_Families_Households_Dwellings_CT":
                        duplicate(name, fc, outputFolder, 7)

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
        if name == duplicated_name("Dwelling_households_EA", 0):
            # Define keepField & newName parameters for delete_rename function
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL6", "COL8", "TFvariable", "Z_score"]
            expression0 = "!COL8! / !COL6!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Rental_Dwellings_1991_DA.shp"
        elif name == duplicated_name("Dwelling_households_EA", 1):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL6", "COL17", "TFvariable", "Z_score"]
            expression0 = "!COL17! / !COL6!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Mobile_Dwellings_1991_DA.shp"
        elif name == duplicated_name("Dwelling_households_B_EA", 0):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL9", "TFvariable", "Z_score"]
            expression0 = "!COL9!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Average_Value_Dwellings_1991_DA.shp"
        elif name == duplicated_name("Dwelling_households_B_EA", 1):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL27", "COL34", "TFvariable", "Z_score"]
            expression0 = "!COL34! / !COL27!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Lone_Parent_Dwellings_1991_DA.shp"
        elif name == duplicated_name("Dwelling_households_B_EA", 2):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL6", "COL12", "TFvariable", "Z_score"]
            expression0 = "!COL12! / !COL6!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Major_Repair_Dwellings_1991_DA.shp"
        elif name == duplicated_name("Dwelling_households_B_EA", 3):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL6", "COL13", "COL14", "TFvariable", "Z_score"]
            expression0 = "(!COL13! + !COL14!) / !COL6!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_1960Construction_Dwellings_1991_DA.shp"
        elif name == duplicated_name("Families_EA", 0):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL47", "COL51", "TFvariable", "Z_score"]
            expression0 = "!COL51! / !COL47!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Living_Alone_Families_1991_DA.shp"
        elif name == duplicated_name("Income_EA", 0):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL60", "TFvariable", "Z_score"]
            expression0 = "!COL60!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Household_Average_Income_Income_1991_DA.shp"
        elif name == duplicated_name("Income_EA", 1):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL54", "TFvariable", "Z_score"]
            expression0 = "!COL54!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Low_Income_Income_1991_DA.shp"
        elif name == duplicated_name("Income_EA", 2):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL20", "TFvariable", "Z_score"]
            expression0 = "!COL20!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Government_Transfers_Income_1991_DA.shp"
        elif name == duplicated_name("Labor_activity_B_EA", 0):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL7", "COL9", "TFvariable", "Z_score"]
            expression0 = "!COL9! / !COL7!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Labour_Unemployment_Labour_1991_DA.shp"
        elif name == duplicated_name("Language_B_EA_", 0):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL6", "COL45", "TFvariable", "Z_score"]
            expression0 = "!COL45! / !COL6!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Neither_Language_Language_1991_DA.shp"
        elif name == duplicated_name("Reli_ethnic_ori_citzn_immi_B_EA_", 0):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL6", "COL87", "TFvariable", "Z_score"]
            expression0 = "!COL87! / !COL6!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Immigration_Religion_1991_DA.shp"
        elif name == duplicated_name("Reli_ethnic_ori_citzn_immi_B_EA_", 1):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL6", "COL42", "TFvariable", "Z_score"]
            expression0 = "!COL42! / !COL6!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Canadian_Citizen_Religion_1991_DA.shp"

        elif name == duplicated_name("Citizen_Immigration_Migration_CT", 0):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL4", "COL63", "TFvariable", "Z_score"]
            # arcpy.AddField_management(path, "COL4_", "DOUBLE", "", "", 50)
            # arcpy.CalculateField_management(path, "COL4_", "!COL4!", "PYTHON_9.3")
            # arcpy.AddField_management(path, "COL63_", "DOUBLE", "", "", 50)
            # arcpy.CalculateField_management(path, "COL63_", "!COL63!", "PYTHON_9.3")
            field_process1(path, "COL4_", "COL63_", "!COL4!", "!COL63!")
            expression0 = "!COL63_! / !COL4_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Immigration_Citizen_1991_CT.shp"
        elif name == duplicated_name("Citizen_Immigration_Migration_CT", 1):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL7", "COL42", "TFvariable", "Z_score"]
            field_process1(path, "COL7_", "COL42_", "!COL7!", "!COL42!")
            expression0 = "!COL42_! / !COL7_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Canadian_Citizen_Citizen_1991_CT.shp"
        elif name == duplicated_name("Education_income_CT", 0):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL160", "TFvariable", "Z_score"]
            expression0 = "!COL160!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Household_Average_Income_Education_1991_CT.shp"
        elif name == duplicated_name("Education_income_CT", 1):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL149", "TFvariable", "Z_score"]
            expression0 = "!COL149!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Low_Income_Education_1991_CT.shp"
        elif name == duplicated_name("Education_income_CT", 2):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL95", "TFvariable", "Z_score"]
            expression0 = "!COL95!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Government_Transfers_Education_1991_CT.shp"
        elif name == duplicated_name("Labour_Employment_Occupation_CT", 0):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL43", "COL45", "TFvariable", "Z_score"]
            field_process1(path, "COL43_", "COL45_", "!COL43!", "!COL45!")
            expression0 = "!COL45_! / !COL43_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Labour_Unemployment_Labour_1991_CT.shp"
        elif name == duplicated_name("Language_CT", 0):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL4", "COL117", "TFvariable", "Z_score"]
            field_process1(path, "COL4_", "COL117_", "!COL4!", "!COL117!")
            expression0 = "!COL117_! / !COL4_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Neither_Language_Language_1991_CT.shp"
        elif name == duplicated_name("Marital_Families_Households_Dwellings_CT", 0):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL47", "COL49", "TFvariable", "Z_score"]
            field_process1(path, "COL47_", "COL49_", "!COL47!", "!COL49!")
            expression0 = "!COL49_! / !COL47_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Rental_Marital_1991_CT.shp"
        elif name == duplicated_name("Marital_Families_Households_Dwellings_CT", 1):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL141", "TFvariable", "Z_score"]
            field_process0(path, "COL141_", "!COL141!")
            expression0 = "!COL141_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Average_Value_Marital_1991_CT.shp"
        elif name == duplicated_name("Marital_Families_Households_Dwellings_CT", 2):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL70", "COL88", "TFvariable", "Z_score"]
            field_process1(path, "COL70_", "COL88_", "!COL70!", "!COL88!")
            expression0 = "!COL88_! / !COL70_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Lone_Parent_Marital_1991_CT.shp"
        elif name == duplicated_name("Marital_Families_Households_Dwellings_CT", 3):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL111", "COL115", "TFvariable", "Z_score"]
            field_process1(path, "COL111_", "COL115_", "!COL111!", "!COL115!")
            expression0 = "!COL115_! / !COL111_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Living_Alone_Marital_1991_CT.shp"
        elif name == duplicated_name("Marital_Families_Households_Dwellings_CT", 4):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL138", "COL143", "TFvariable", "Z_score"]
            field_process1(path, "COL138_", "COL143_", "!COL138!", "!COL143!")
            expression0 = "!COL143_! / !COL138_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Major_Repair_Marital_1991_CT.shp"
        elif name == duplicated_name("Marital_Families_Households_Dwellings_CT", 5):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL47", "COL58", "TFvariable", "Z_score"]
            field_process1(path, "COL47_", "COL58_", "!COL47!", "!COL58!")
            expression0 = "!COL58_! / !COL47_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Mobile_Marital_1991_CT.shp"
        elif name == duplicated_name("Marital_Families_Households_Dwellings_CT", 6):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL138", "COL145", "COL146", "TFvariable", "Z_score"]
            field_process2(path, "COL138_", "COL145_", "COL146_", "!COL138!", "!COL145!", "!COL146!")
            expression0 = "(!COL145_! + !COL146_!)/ !COL138_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_1960Constructions_Marital_1991_CT.shp"
        else:
            continue

        # Import delete_rename function only once instead execute in every lower tier if condition
        data_process(path, keepFields, expression0, expression1, newName)

if __name__ == '__main__':
    fcs_in_workspace(inputFolder)
