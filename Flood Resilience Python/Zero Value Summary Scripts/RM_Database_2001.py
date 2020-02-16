
# Import system modules
import arcpy
import os
import sys

arcpy.env.overwriteOutput = True

# Read the parameter values
# inputFolder = arcpy.GetParameterAsText(0)
# outputFolder = arcpy.GetParameterAsText(1)
inputFolder = "D:\\MSc Thesis\\CCAR Database\\2001\\Social_Resilience_2001_Input"
outputFolder = "D:\\MSc Thesis\\CCAR Database\\Missing_Values\\2001"

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
def data_process(path, keepFields, newName):
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

# # Define variable calculate function using variables of path, fieldName
# def variable_calculate(path, fieldName):
#     statsfield = arcpy.da.FeatureClassToNumPyArray(path, fieldName)
#     u = statsfield[fieldName].mean()
#     sd = statsfield[fieldName].std()
#     return "(!%s! - %f) / %f" % (fieldName, u, sd)
#     # factor = u / sd
#     # return "!%s! * %f" % (fieldName, factor)

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
                    if name == "Citizenship_DA":
                        duplicate(name, fc, outputFolder, 2)
                    elif name == "Families_DA":
                        duplicate(name, fc, outputFolder, 1)
                    elif name == "Income_DA":
                        duplicate(name, fc, outputFolder, 3)
                    elif name == "Labour_DA":
                        duplicate(name, fc, outputFolder, 1)
                    elif name == "Language_DA":
                        duplicate(name, fc, outputFolder, 1)
                    elif name == "Marital_DA":
                        duplicate(name, fc, outputFolder, 6)

                    elif name == "Citizenship_CT":
                        duplicate(name, fc, outputFolder, 2)
                    elif name == "Income_CT":
                        duplicate(name, fc, outputFolder, 4)
                    elif name == "Labour_CT":
                        duplicate(name, fc, outputFolder, 1)
                    elif name == "Language_CT":
                        duplicate(name, fc, outputFolder, 1)
                    elif name == "Marital_CT":
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
        # Pre-declare variables of keepFields & newName with empty container for delete_rename function
        # Allow to let the lower tier block of different keepFields & newName codes
        # Fill in the upper tier keepFields & newName
        keepFields = []
        newName = ""

        # Import duplicated_name function to define names for if conditions
        if name == duplicated_name("Citizenship_DA", 0):
            keepFields = ["FID", "Shape", "DAUID", "COUNT", "FIRST_PRUI", "FIRST_CSDU", "FIRST_CMAU", "OID_", "GEOGRAPHY", "1996_2001", "TOTAL_POPU", "_1996_2001", "TOTAL_POP_"]
            field_process1(path, "_1996_2001", "TOTAL_POP_", "!1996_2001!", "!TOTAL_POPU!")
            newName = "Immigration_Citizenship_2001_DA.shp"
        elif name == duplicated_name("Citizenship_DA", 1):
            keepFields = ["FID", "Shape", "DAUID", "COUNT", "FIRST_PRUI", "FIRST_CSDU", "FIRST_CMAU", "OID_", "GEOGRAPHY", "CANADIAN_C", "TOTAL_POPU", "CANADA_C_", "TOTAL_POP_"]
            field_process1(path, "CANADA_C_", "TOTAL_POP_", "!CANADIAN_C!", "!TOTAL_POPU!")
            newName = "Canadian_Citizen_Citizenship_2001_DA.shp"
        elif name == duplicated_name("Families_DA", 0):
            keepFields = ["FID", "Shape", "DAUID", "COUNT", "FIRST_PRUI", "FIRST_CSDU", "FIRST_CMAU", "OID_", "Geography", "Average_va"]
            newName = "Dwellings_Average_Value_Families_2001_DA.shp"
        elif name == duplicated_name("Income_DA", 0):
            keepFields = ["FID", "Shape", "DAUID", "COUNT", "FIRST_PRUI", "FIRST_CSDU", "FIRST_CMAU", "OID_", "GEOGRAPHY", "AVERAGE_7"]
            newName = "Household_Average_Income_Income_2001_DA.shp"
        elif name == duplicated_name("Income_DA", 1):
            keepFields = ["FID", "Shape", "DAUID", "COUNT", "FIRST_PRUI", "FIRST_CSDU", "FIRST_CMAU", "OID_", "GEOGRAPHY", "INCIDENCE_"]
            newName = "Low_Income_Income_2001_DA.shp"
        elif name == duplicated_name("Income_DA", 2):
            keepFields = ["FID", "Shape", "DAUID", "COUNT", "FIRST_PRUI", "FIRST_CSDU", "FIRST_CMAU", "OID_", "GEOGRAPHY", "GOVERNMENT"]
            newName = "Government_Transfers_Income_2001_DA.shp"
        elif name == duplicated_name("Labour_DA", 0):
            keepFields = ["FID", "Shape", "DAUID", "COUNT", "FIRST_PRUI", "FIRST_CSDU", "FIRST_CMAU", "OID_", "GEOGRAPHY", "UNEMPLOYED", "IN_THE_LAB"]
            newName = "Labour_Unemployment_Labour_2001_DA.shp"
        elif name == duplicated_name("Language_DA", 0):
            keepFields = ["FID", "Shape", "DAUID", "COUNT", "FIRST_PRUI", "FIRST_CSDU", "FIRST_CMAU", "OID_", "Geography", "Neither_En", "Total_po_1"]
            newName = "Neither_Language_Language_2001_DA.shp"
        elif name == duplicated_name("Marital_DA", 0):
            keepFields = ["FID", "Shape", "DAUID", "COUNT", "FIRST_PRUI", "FIRST_CSDU", "FIRST_CMAU", "OID_", "GEOGRAPHY", "RENTED", "TOTAL_NU4"]
            newName = "Dwellings_Rental_Martial_2001_DA.shp"
        elif name == duplicated_name("Marital_DA", 1):
            keepFields = ["FID", "Shape", "DAUID", "COUNT", "FIRST_PRUI", "FIRST_CSDU", "FIRST_CMAU", "OID_", "GEOGRAPHY", "TOTAL_LONE", "TOTAL_NUMB"]
            newName = "Lone_Parent_Martial_2001_DA.shp"
        elif name == duplicated_name("Marital_DA", 2):
            keepFields = ["FID", "Shape", "DAUID", "COUNT", "FIRST_PRUI", "FIRST_CSDU", "FIRST_CMAU", "OID_", "GEOGRAPHY", "LIVING_AL1", "TOTAL_NU3"]
            newName = "Living_Alone_Martial_2001_DA.shp"
        elif name == duplicated_name("Marital_DA", 3):
            keepFields = ["FID", "Shape", "DAUID", "COUNT", "FIRST_PRUI", "FIRST_CSDU", "FIRST_CMAU", "OID_", "GEOGRAPHY", "MAJOR_REPA", "TOTAL_NU4"]
            newName = "Dwellings_Major_Repair_Marital_2001_DA.shp"
        elif name == duplicated_name("Marital_DA", 4):
            keepFields = ["FID", "Shape", "DAUID", "COUNT", "FIRST_PRUI", "FIRST_CSDU", "FIRST_CMAU", "OID_", "GEOGRAPHY", "MOVABLE_DW", "TOTAL_NU5"]
            newName = "Dwellings_Mobile_Marital_2001_DA.shp"
        elif name == duplicated_name("Marital_DA", 5):
            keepFields = ["FID", "Shape", "DAUID", "COUNT", "FIRST_PRUI", "FIRST_CSDU", "FIRST_CMAU", "OID_", "GEOGRAPHY", "PERIOD_OF_", "PERIOD_OF1", "TOTAL_NU4"]
            newName = "Dwellings_1960Constructions_Marital_2001_DA.shp"
        else:
            continue

        # Import delete_rename function only once instead execute in every lower tier if condition
        data_process(path, keepFields, newName)

if __name__ == '__main__':
    fcs_in_workspace(inputFolder)
