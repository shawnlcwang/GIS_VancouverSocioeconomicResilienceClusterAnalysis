
# Import system modules
import arcpy
import os
import sys

arcpy.env.overwriteOutput = True

# Read the parameter values
# inputFolder = arcpy.GetParameterAsText(0)
# outputFolder = arcpy.GetParameterAsText(1)
inputFolder = "D:\\MSc Thesis\\CCAR Raw Data\\1991"
outputFolder = "D:\\MSc Thesis\\CCAR Database\\1991\\Social_Resilience_1991_Output"

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
    if name == duplicated_name("Dwellings_EA", 0):
        cursor = arcpy.da.UpdateCursor(path, ["COL6", "COL8", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Dwellings_EA", 1):
        cursor = arcpy.da.UpdateCursor(path, ["POPULATION", "COL9", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Dwellings_EA", 2):
        cursor = arcpy.da.UpdateCursor(path, ["COL27", "COL34", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Dwellings_EA", 3):
        cursor = arcpy.da.UpdateCursor(path, ["COL6", "COL12", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Dwellings_EA", 4):
        cursor = arcpy.da.UpdateCursor(path, ["COL6", "COL17", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Dwellings_EA", 5):
        cursor = arcpy.da.UpdateCursor(path, ["COL6", "COL13", "COL14", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Families_EA", 0):
        cursor = arcpy.da.UpdateCursor(path, ["COL47", "COL51", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Income_EA", 0):
        u0 = value_calculate(path, "Income_EA", 0, 1, "COL60", "COL60_", "!COL60!")
        cursor = arcpy.da.UpdateCursor(path, ["POPULATION", "COL60_", "Tvariable"])
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
        cursor = arcpy.da.UpdateCursor(path, ["POPULATION", "COL54", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Income_EA", 2):
        cursor = arcpy.da.UpdateCursor(path, ["POPULATION", "COL20", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Labour_EA", 0):
        u0 = value_calculate(path, "Labour_EA", 0, 1, "COL7", "COL7_", "!COL7!")
        u1 = value_calculate(path, "Labour_EA", 0, 2, "COL9", "COL9_", "!COL9!")
        cursor = arcpy.da.UpdateCursor(path, ["COL7_", "COL9_", "Tvariable"])
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
        u0 = value_calculate(path, "Language_EA", 0, 1, "COL6", "COL6_", "!COL6!")
        u1 = value_calculate(path, "Language_EA", 0, 2, "COL45", "COL45_", "!COL45!")
        cursor = arcpy.da.UpdateCursor(path, ["COL6_", "COL45_", "Tvariable"])
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
    elif name == duplicated_name("Religion_EA", 0):
        cursor = arcpy.da.UpdateCursor(path, ["COL6", "COL87", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Religion_EA", 1):
        cursor = arcpy.da.UpdateCursor(path, ["COL6", "COL42", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor

    elif name == duplicated_name("Citizen_CT", 0):
        cursor = arcpy.da.UpdateCursor(path, ["COL4", "COL63", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Citizen_CT", 1):
        cursor = arcpy.da.UpdateCursor(path, ["COL7", "COL42", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Education_CT", 0):
        cursor = arcpy.da.UpdateCursor(path, ["POPULATION", "COL160", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Education_CT", 1):
        cursor = arcpy.da.UpdateCursor(path, ["POPULATION", "COL149", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Education_CT", 2):
        cursor = arcpy.da.UpdateCursor(path, ["POPULATION", "COL95", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Labour_CT", 0):
        cursor = arcpy.da.UpdateCursor(path, ["COL43", "COL45", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Language_CT", 0):
        cursor = arcpy.da.UpdateCursor(path, ["COL4", "COL117", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Marital_CT", 0):
        cursor = arcpy.da.UpdateCursor(path, ["COL47", "COL49", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Marital_CT", 1):
        cursor = arcpy.da.UpdateCursor(path, ["POPULATION", "COL141", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Marital_CT", 2):
        cursor = arcpy.da.UpdateCursor(path, ["COL70", "COL88", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Marital_CT", 3):
        cursor = arcpy.da.UpdateCursor(path, ["COL111", "COL115", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Marital_CT", 4):
        cursor = arcpy.da.UpdateCursor(path, ["COL144", "COL138", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Marital_CT", 5):
        cursor = arcpy.da.UpdateCursor(path, ["COL47", "COL58", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
        del row
        del cursor
    elif name == duplicated_name("Marital_CT", 6):
        cursor = arcpy.da.UpdateCursor(path, ["COL138", "COL145", "COL146", "Tvariable"])
        for row in cursor:
            if row[0] == 0:
                cursor.deleteRow()
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
                    if name == "Dwellings_EA":
                        duplicate(name, fc, outputFolder, 6)
                    elif name == "Families_EA":
                        duplicate(name, fc, outputFolder, 1)
                    elif name == "Income_EA":
                        duplicate(name, fc, outputFolder, 3)
                    elif name == "Labour_EA":
                        duplicate(name, fc, outputFolder, 1)
                    elif name == "Language_EA":
                        duplicate(name, fc, outputFolder, 1)
                    elif name == "Religion_EA":
                        duplicate(name, fc, outputFolder, 2)

                    elif name == "Citizen_CT":
                        duplicate(name, fc, outputFolder, 2)
                    elif name == "Education_CT":
                        duplicate(name, fc, outputFolder, 3)
                    elif name == "Labour_CT":
                        duplicate(name, fc, outputFolder, 1)
                    elif name == "Language_CT":
                        duplicate(name, fc, outputFolder, 1)
                    elif name == "Marital_CT":
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
        if name == duplicated_name("Dwellings_EA", 0):
            # Define keepField & newName parameters for delete_rename function
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "GEOGRAPHY", "COL6",  "COL8", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL8! / !COL6!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Rental_Dwellings_1991_EA.shp"
        elif name == duplicated_name("Dwellings_EA", 1):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "GEOGRAPHY", "COL9", "POPULATION", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL9!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Average_Value_Dwellings_1991_EA.shp"
        elif name == duplicated_name("Dwellings_EA", 2):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "GEOGRAPHY", "COL27", "COL34", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL34! / !COL27!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Lone_Parent_Dwellings_1991_EA.shp"
        elif name == duplicated_name("Dwellings_EA", 3):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "GEOGRAPHY", "COL6", "COL12", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL12! / !COL6!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Major_Repair_Dwellings_1991_EA.shp"
        elif name == duplicated_name("Dwellings_EA", 4):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "GEOGRAPHY", "COL6", "COL17", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL17! / !COL6!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Mobile_Dwellings_1991_EA.shp"
        elif name == duplicated_name("Dwellings_EA", 5):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "GEOGRAPHY", "COL6", "COL13", "COL14", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "(!COL13! + !COL14!) / !COL6!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_1960Construction_Dwellings_1991_EA.shp"
        elif name == duplicated_name("Families_EA", 0):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "GEOGRAPHY", "COL47", "COL51", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL51! / !COL47!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Living_Alone_Families_1991_EA.shp"
        elif name == duplicated_name("Income_EA", 0):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "GEOGRAPHY", "COL60", "COL60_", "POPULATION", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL60_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Household_Average_Income_Income_1991_EA.shp"
        elif name == duplicated_name("Income_EA", 1):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "GEOGRAPHY", "COL54", "POPULATION", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL54!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Low_Income_Income_1991_EA.shp"
        elif name == duplicated_name("Income_EA", 2):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "GEOGRAPHY", "COL20", "POPULATION", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL20!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Government_Transfers_Income_1991_EA.shp"
        elif name == duplicated_name("Labour_EA", 0):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "GEOGRAPHY", "COL7", "COL7_", "COL9", "COL9_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL9_! / !COL7_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Labour_Unemployment_Labour_1991_EA.shp"
        elif name == duplicated_name("Language_EA", 0):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "GEOGRAPHY", "COL6", "COL6_", "COL45", "COL45_", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL45_! / !COL6_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Neither_Language_Language_1991_EA.shp"
        elif name == duplicated_name("Religion_EA", 0):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "GEOGRAPHY", "COL6", "COL87", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL87! / !COL6!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Immigration_Religion_1991_EA.shp"
        elif name == duplicated_name("Religion_EA", 1):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "GEOGRAPHY", "COL6", "COL42", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL42! / !COL6!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Canadian_Citizen_Religion_1991_EA.shp"

        elif name == duplicated_name("Citizen_CT", 0):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "GEOGRAPHY", "COL4", "COL63", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL63! / !COL4!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Immigration_Citizen_1991_CT.shp"
        elif name == duplicated_name("Citizen_CT", 1):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "GEOGRAPHY", "COL7", "COL42", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL42! / !COL7!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Canadian_Citizen_Citizen_1991_CT.shp"
        elif name == duplicated_name("Education_CT", 0):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "GEOGRAPHY", "COL160", "POPULATION", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL160!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Household_Average_Income_Education_1991_CT.shp"
        elif name == duplicated_name("Education_CT", 1):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "GEOGRAPHY", "COL149", "POPULATION", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL149!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Low_Income_Education_1991_CT.shp"
        elif name == duplicated_name("Education_CT", 2):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "GEOGRAPHY", "COL95", "POPULATION", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL95!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Government_Transfers_Education_1991_CT.shp"
        elif name == duplicated_name("Labour_CT", 0):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "GEOGRAPHY", "COL43", "COL45", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL45! / !COL43!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Labour_Unemployment_Labour_1991_CT.shp"
        elif name == duplicated_name("Language_CT", 0):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "GEOGRAPHY", "COL4", "COL117", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL117! / !COL4!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Neither_Language_Language_1991_CT.shp"
        elif name == duplicated_name("Marital_CT", 0):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "GEOGRAPHY", "COL47", "COL49", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL49! / !COL47!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Rental_Marital_1991_CT.shp"
        elif name == duplicated_name("Marital_CT", 1):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "GEOGRAPHY", "COL141", "POPULATION", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL141!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Average_Value_Marital_1991_CT.shp"
        elif name == duplicated_name("Marital_CT", 2):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "GEOGRAPHY", "COL70", "COL88", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL88! / !COL70!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Lone_Parent_Marital_1991_CT.shp"
        elif name == duplicated_name("Marital_CT", 3):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "GEOGRAPHY", "COL111", "COL115", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL115! / !COL111!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Living_Alone_Marital_1991_CT.shp"
        elif name == duplicated_name("Marital_CT", 4):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "GEOGRAPHY", "COL138", "COL144", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL144! / !COL138!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Major_Repair_Marital_1991_CT.shp"
        elif name == duplicated_name("Marital_CT", 5):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "GEOGRAPHY", "COL47", "COL58", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "!COL58! / !COL47!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Mobile_Marital_1991_CT.shp"
        elif name == duplicated_name("Marital_CT", 6):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "GEOGRAPHY", "COL138", "COL145", "COL146", "Tvariable", "TFvariable", "Z_score"]
            expression0 = "(!COL145! + !COL146!)/ !COL138!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_1960Constructions_Marital_1991_CT.shp"
        else:
            continue

        # Import delete_rename function only once instead execute in every lower tier if condition
        data_process(path, keepFields, expression0, expression1, newName)

if __name__ == '__main__':
    fcs_in_workspace(inputFolder)
