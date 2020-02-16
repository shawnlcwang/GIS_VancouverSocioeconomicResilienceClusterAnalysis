
# Import system modules
import arcpy
import os
import sys

arcpy.env.overwriteOutput = True

# Read the parameter values
# inputFolder = arcpy.GetParameterAsText(0)
# outputFolder = arcpy.GetParameterAsText(1)
inputFolder = "D:\\MSc Thesis\\CCAR Database\\Database_v2_MRZE\\2011\\Social_Resilience_2011_Input"
outputFolder = "D:\\MSc Thesis\\CCAR Database\\Database_v2_MRZE\\2011\\Social_Resilience_2011_Output"


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

# Define zero value process function using variables of path
def zero_process0(path, fieldName1):
    cursor = arcpy.da.UpdateCursor(path, [fieldName1])
    for row in cursor:
        if row[0] == 0:
            cursor.deleteRow()
    del row
    del cursor

# Define zero value process function using variables of path
def zero_process1(path, fieldName1, fieldName2):
    cursor = arcpy.da.UpdateCursor(path, [fieldName1, fieldName2])
    for row in cursor:
        if row[0] == 0:
            cursor.deleteRow()
        elif row[1] == 0:
            cursor.deleteRow()
    del row
    del cursor

# Define zero value process function using variables of path
def zero_process2(path, fieldName1, fieldName2, fieldName3):
    cursor = arcpy.da.UpdateCursor(path, [fieldName1, fieldName2, fieldName3])
    for row in cursor:
        if row[0] == 0:
            cursor.deleteRow()
        elif row[1] == 0:
            cursor.deleteRow()
        elif row[2] == 0:
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
    # factor = u / sd
    # return "!%s! * %f" % (fieldName, factor)

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
            keepFields = ["FID", "Shape", "DAUID", "CDUID", "CDNAME", "CDTYPE", "CSDUID", "CSDNAME", "CSDTYPE", "CCSUID", "CCSNAME", "ERUID", "ERNAME", "CMAPUID", "CMAUID", "CMANAME", "CMATYPE", "SACCODE", "SACTYPE", "CTUID", "CTNAME", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "AVERAGE_HO", "TFvariable", "Z_score"]
            zero_process0(path, "AVERAGE_HO")
            expression0 = "!AVERAGE_HO!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Household_Average_Income_Income_2011_DA.shp"
        elif name == duplicated_name("Income_DA_", 1):
            keepFields = ["FID", "Shape", "DAUID", "CDUID", "CDNAME", "CDTYPE", "CSDUID", "CSDNAME", "CSDTYPE", "CCSUID", "CCSNAME", "ERUID", "ERNAME", "CMAPUID", "CMAUID", "CMANAME", "CMATYPE", "SACCODE", "SACTYPE", "CTUID", "CTNAME", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "RENTER", "TOTAL_NU4", "TFvariable", "Z_score"]
            zero_process1(path, "RENTER", "TOTAL_NU4")
            expression0 = "!RENTER! / !TOTAL_NU4!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Rental_Income_2011_DA.shp"
        elif name == duplicated_name("Income_DA_", 2):
            keepFields = ["FID", "Shape", "DAUID", "CDUID", "CDNAME", "CDTYPE", "CSDUID", "CSDNAME", "CSDTYPE", "CCSUID", "CCSNAME", "ERUID", "ERNAME", "CMAPUID", "CMAUID", "CMANAME", "CMATYPE", "SACCODE", "SACTYPE", "CTUID", "CTNAME", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "PREVALENCE", "TFvariable", "Z_score"]
            zero_process0(path, "PREVALENCE")
            expression0 = "!PREVALENCE!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Low_Income_Income_2011_DA.shp"
        elif name == duplicated_name("Income_DA_", 3):
            keepFields = ["FID", "Shape", "DAUID", "CDUID", "CDNAME", "CDTYPE", "CSDUID", "CSDNAME", "CSDTYPE", "CCSUID", "CCSNAME", "ERUID", "ERNAME", "CMAPUID", "CMAUID", "CMANAME", "CMATYPE", "SACCODE", "SACTYPE", "CTUID", "CTNAME", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "GOVERNMENT", "TFvariable", "Z_score"]
            zero_process0(path, "GOVERNMENT")
            expression0 = "!GOVERNMENT!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Government_Transfers_Income_2011_DA.shp"
        elif name == duplicated_name("Income_DA_", 4):
            keepFields = ["FID", "Shape", "DAUID", "CDUID", "CDNAME", "CDTYPE", "CSDUID", "CSDNAME", "CSDTYPE", "CCSUID", "CCSNAME", "ERUID", "ERNAME", "CMAPUID", "CMAUID", "CMANAME", "CMATYPE", "SACCODE", "SACTYPE", "CTUID", "CTNAME", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "AVERAGE_VA", "TFvariable", "Z_score"]
            zero_process0(path, "AVERAGE_VA")
            expression0 = "!AVERAGE_VA!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Average_Value_Income_2011_DA.shp"
        elif name == duplicated_name("Income_DA_", 5):
            keepFields = ["FID", "Shape", "DAUID", "CDUID", "CDNAME", "CDTYPE", "CSDUID", "CSDNAME", "CSDTYPE", "CCSUID", "CCSNAME", "ERUID", "ERNAME", "CMAPUID", "CMAUID", "CMANAME", "CMATYPE", "SACCODE", "SACTYPE", "CTUID", "CTNAME", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "MAJOR_REPA", "TOTAL_NUMB", "TFvariable", "Z_score"]
            zero_process1(path, "MAJOR_REPA", "TOTAL_NUMB")
            expression0 = "!MAJOR_REPA! / !TOTAL_NUMB!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Major_Repair_Income_2011_DA.shp"
        elif name == duplicated_name("Income_DA_", 6):
            keepFields = ["FID", "Shape", "DAUID", "CDUID", "CDNAME", "CDTYPE", "CSDUID", "CSDNAME", "CSDTYPE", "CCSUID", "CCSNAME", "ERUID", "ERNAME", "CMAPUID", "CMAUID", "CMANAME", "CMATYPE", "SACCODE", "SACTYPE", "CTUID", "CTNAME", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "1960_OR_BE", "TOTAL_NUM1", "TFvariable", "Z_score"]
            zero_process1(path, "1960_OR_BE", "TOTAL_NUM1")
            expression0 = "!1960_OR_BE! / !TOTAL_NUM1!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_1960Constructions_Income_2011_DA.shp"
        elif name == duplicated_name("Marital_Language_DA", 0):
            keepFields = ["FID", "Shape", "DAUID", "CDUID", "CDNAME", "CDTYPE", "CSDUID", "CSDNAME", "CSDTYPE", "CCSUID", "CCSNAME", "ERUID", "ERNAME", "CMAPUID", "CMAUID", "CMANAME", "CMATYPE", "SACCODE", "SACTYPE", "CTUID", "CTNAME", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "NEITHER_EN", "KNOWLEDGE_", "TFvariable", "Z_score"]
            zero_process1(path, "NEITHER_EN", "KNOWLEDGE_")
            expression0 = "!NEITHER_EN! / !KNOWLEDGE_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Neither_Language_Language_2011_DA.shp"
        elif name == duplicated_name("Marital_Language_DA", 1):
            keepFields = ["FID", "Shape", "DAUID", "CDUID", "CDNAME", "CDTYPE", "CSDUID", "CSDNAME", "CSDTYPE", "CCSUID", "CCSNAME", "ERUID", "ERNAME", "CMAPUID", "CMAUID", "CMANAME", "CMATYPE", "SACCODE", "SACTYPE", "CTUID", "CTNAME", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "TOTAL_LONE", "TOTAL_NUMB", "TFvariable", "Z_score"]
            zero_process1(path, "TOTAL_LONE", "TOTAL_NUMB")
            expression0 = "!TOTAL_LONE! / !TOTAL_NUMB!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Lone_Parent_Marital_2011_DA.shp"
        elif name == duplicated_name("Marital_Language_DA", 2):
            keepFields = ["FID", "Shape", "DAUID", "CDUID", "CDNAME", "CDTYPE", "CSDUID", "CSDNAME", "CSDTYPE", "CCSUID", "CCSNAME", "ERUID", "ERNAME", "CMAPUID", "CMAUID", "CMANAME", "CMATYPE", "SACCODE", "SACTYPE", "CTUID", "CTNAME", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "LIVING_AL1", "TOTAL_NU3", "TFvariable", "Z_score"]
            zero_process1(path, "LIVING_AL1", "TOTAL_NU3")
            expression0 = "!LIVING_AL1! / !TOTAL_NU3!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Living_Alone_Marital_2011_DA.shp"
        elif name == duplicated_name("Marital_Language_DA", 3):
            keepFields = ["FID", "Shape", "DAUID", "CDUID", "CDNAME", "CDTYPE", "CSDUID", "CSDNAME", "CSDTYPE", "CCSUID", "CCSNAME", "ERUID", "ERNAME", "CMAPUID", "CMAUID", "CMANAME", "CMATYPE", "SACCODE", "SACTYPE", "CTUID", "CTNAME", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "MOVABLE_DW", "TOTAL_NU5", "TFvariable", "Z_score"]
            zero_process1(path, "MOVABLE_DW", "TOTAL_NU5")
            expression0 = "!MOVABLE_DW! / !TOTAL_NU5!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Mobile_Marital_2011_DA.shp"

        elif name == duplicated_name("Income_Labour_Immigration_CT", 0):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "AVERAGE_HO", "TFvariable", "Z_score"]
            zero_process0(path, "AVERAGE_HO")
            expression0 = "!AVERAGE_HO!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Household_Average_Income_Income_2011_CT.shp"
        elif name == duplicated_name("Income_Labour_Immigration_CT", 1):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "UNEMPLOYED", "IN_THE_LAB", "TFvariable", "Z_score"]
            zero_process1(path, "UNEMPLOYED", "IN_THE_LAB")
            expression0 = "!UNEMPLOYED! / !IN_THE_LAB!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Labour_Unemployment_Education_2011_CT.shp"
        elif name == duplicated_name("Income_Labour_Immigration_CT", 2):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "RENTER", "TOTAL_NU4", "TFvariable", "Z_score"]
            zero_process1(path, "RENTER", "TOTAL_NU4")
            expression0 = "!RENTER! / !TOTAL_NU4!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Rental_Income_2011_CT.shp"
        elif name == duplicated_name("Income_Labour_Immigration_CT", 3):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "PREVALENCE", "TFvariable", "Z_score"]
            zero_process0(path, "PREVALENCE")
            expression0 = "!PREVALENCE!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Low_Income_Income_2011_CT.shp"
        elif name == duplicated_name("Income_Labour_Immigration_CT", 4):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "GOVERNMENT", "TFvariable", "Z_score"]
            zero_process0(path, "GOVERNMENT")
            expression0 = "!GOVERNMENT!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Government_Transfers_Income_2011_CT.shp"
        elif name == duplicated_name("Income_Labour_Immigration_CT", 5):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "AVERAGE_VA", "TFvariable", "Z_score"]
            zero_process0(path, "AVERAGE_VA")
            expression0 = "!AVERAGE_VA!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Average_Value_Income_2011_CT.shp"
        elif name == duplicated_name("Income_Labour_Immigration_CT", 6):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "MAJOR_REPA", "TOTAL_NUMB", "TFvariable", "Z_score"]
            zero_process1(path, "MAJOR_REPA", "TOTAL_NUMB")
            expression0 = "!MAJOR_REPA! / !TOTAL_NUMB!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_Major_Repair_Income_2011_CT.shp"
        elif name == duplicated_name("Income_Labour_Immigration_CT", 7):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "1960_OR_BE", "TOTAL_NUM1", "TFvariable", "Z_score"]
            zero_process1(path, "1960_OR_BE", "TOTAL_NUM1")
            expression0 = "!1960_OR_BE! / !TOTAL_NUM1!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Dwellings_1960Constructions_Income_2011_CT.shp"
        elif name == duplicated_name("Income_Labour_Immigration_CT", 8):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "2006_TO_20", "TOTAL_POP1", "TFvariable", "Z_score"]
            zero_process1(path, "2006_TO_20", "TOTAL_POP1")
            expression0 = "!2006_TO_20! / !TOTAL_POP1!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Immigration_Immigration_2011_CT.shp"
        elif name == duplicated_name("Income_Labour_Immigration_CT", 9):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "CANADIAN_C", "TOTAL_PO_1", "TFvariable", "Z_score"]
            zero_process1(path, "CANADIAN_C", "TOTAL_PO_1")
            expression0 = "!CANADIAN_C! / !TOTAL_PO_1!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Canadian_Citizen_Immigration_2011_CT.shp"
        elif name == duplicated_name("Marital_Language_CT", 0):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "NEITHER_EN", "KNOWLEDGE_", "TFvariable", "Z_score"]
            zero_process1(path, "NEITHER_EN", "KNOWLEDGE_")
            expression0 = "!NEITHER_EN! / !KNOWLEDGE_!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Neither_Language_Language_2011_CT.shp"
        elif name == duplicated_name("Marital_Language_CT", 1):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "TOTAL_LONE", "TOTAL_NUMB", "TFvariable", "Z_score"]
            zero_process1(path, "TOTAL_LONE", "TOTAL_NUMB")
            expression0 = "!TOTAL_LONE! / !TOTAL_NUMB!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Lone_Parent_Marital_2011_CT.shp"
        elif name == duplicated_name("Marital_Language_CT", 2):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "LIVING_AL1", "TOTAL_NU3", "TFvariable", "Z_score"]
            zero_process1(path, "LIVING_AL1", "TOTAL_NU3")
            expression0 = "!LIVING_AL1! / !TOTAL_NU3!"
            arcpy.CalculateField_management(path, "normalize", expression0, "PYTHON_9.3")
            expression1 = variable_calculate(path, "normalize")
            newName = "Living_Alone_Marital_2011_CT.shp"
        elif name == duplicated_name("Marital_Language_CT", 3):
            keepFields = ["FID", "Shape", "CTUID", "CTNAME", "CMAUID", "CMANAME", "CMATYPE", "CMAPUID", "PRUID", "PRNAME", "OID_", "GEOGRAPHY", "MOVABLE_DW", "TOTAL_NU5",  "TFvariable", "Z_score"]
            zero_process1(path, "MOVABLE_DW", "TOTAL_NU5")
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
