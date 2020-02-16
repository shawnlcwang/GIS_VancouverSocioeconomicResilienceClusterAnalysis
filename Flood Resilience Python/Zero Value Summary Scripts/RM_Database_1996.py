
# Import system modules
import arcpy
import os
import sys

arcpy.env.overwriteOutput = True

# Read the parameter values
# inputFolder = arcpy.GetParameterAsText(0)
# outputFolder = arcpy.GetParameterAsText(1)
inputFolder = "D:\\MSc Thesis\\CCAR Database\\1996\\Social_Resilience_1996_Input"
outputFolder = "D:\\MSc Thesis\\CCAR Database\\Missing_Values\\1996"

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
                    if name == "Income_EA":
                        duplicate(name, fc, outputFolder, 5)
                    elif name == "Labour_Occupation_Education_EA":
                        duplicate(name, fc, outputFolder, 1)
                    elif name == "Language_Immigration_Citizenships_EA":
                        duplicate(name, fc, outputFolder, 3)
                    elif name == "Marital_Families_Households_EA":
                        duplicate(name, fc, outputFolder, 3)

                    elif name == "Income_CT":
                        duplicate(name, fc, outputFolder, 7)
                    elif name == "Labour_Occupation_Education_CT":
                        duplicate(name, fc, outputFolder, 1)
                    elif name == "Language_Immigration_Citizenships_CT":
                        duplicate(name, fc, outputFolder, 3)
                    elif name == "Marital_Families_Households_CT":
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
        # Pre-declare variables of keepFields & newName with empty container for delete_rename function
        # Allow to let the lower tier block of different keepFields & newName codes
        # Fill in the upper tier keepFields & newName
        keepFields = []
        newName = ""

        # Import duplicated_name function to define names for if conditions
        if name == duplicated_name("Income_EA", 0):
            # Define keepField & newName parameters for delete_rename function
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "AVERAGE31"]
            newName = "Household_Average_Income_Income_1996_EA.shp"
        elif name == duplicated_name("Income_EA", 1):
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "RENTED", "TOTAL_NU4"]
            newName = "Dwellings_Rental_Income_1996_EA.shp"
        elif name == duplicated_name("Income_EA", 2):
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "AVERAGE_VA"]
            newName = "Dwellings_Average_Value_Income_1996_EA.shp"
        elif name == duplicated_name("Income_EA", 3):
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "MAJOR_REPA", "TOTAL_NU4"]
            newName = "Dwellings_Major_Repair_Income_1996_EA.shp"
        elif name == duplicated_name("Income_EA", 4):
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "PERIOD_OF_", "PERIOD_OF1", "TOTAL_NU4"]
            newName = "Dwellings_1960Constructions_Income_1991_EA.shp"
        elif name == duplicated_name("Labour_Occupation_Education_EA", 0):
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "UNEMPLOYED", "IN_THE_LAB"]
            newName = "Labour_Unemployment_Labour_1996_EA.shp"
        elif name == duplicated_name("Language_Immigration_Citizenships_EA", 0):
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "1991_1996_", "TOTAL_POPU"]
            newName = "Immigration_Language_1996_EA.shp"
        elif name == duplicated_name("Language_Immigration_Citizenships_EA", 1):
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "NEITHER_EN", "TOTAL_PO3"]
            newName = "Neither_Language_Language_1996_EA.shp"
        elif name == duplicated_name("Language_Immigration_Citizenships_EA", 2):
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "CANADIAN_C", "TOTAL_POPU"]
            newName = "Canadian_Citizen_Language_1996_EA.shp"
        elif name == duplicated_name("Marital_Families_Households_EA", 0):
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "TOTAL_LONE", "TOTAL_NUMB"]
            newName = "Lone_Parent_Marital_1996_EA.shp"
        elif name == duplicated_name("Marital_Families_Households_EA", 1):
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "LIVING_AL1", "TOTAL_NU3"]
            newName = "Living_Alone_Marital_1996_EA.shp"
        elif name == duplicated_name("Marital_Families_Households_EA", 2):
            keepFields = ["FID", "Shape", "EAUID", "GEOGRAPHY", "MOVABLE_DW", "TOTAL_NU4"]
            newName = "Dwellings_Mobile_Martial_1996_EA.shp"

        elif name == duplicated_name("Income_CT", 0):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "AVERAGE_HO"]
            newName = "Household_Average_Income_Income_1996_CT.shp"
        elif name == duplicated_name("Income_CT", 1):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "RENTED", "TOTAL_NU4"]
            newName = "Dwellings_Rental_Income_1996_CT.shp"
        elif name == duplicated_name("Income_CT", 2):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "INCIDENC2"]
            newName = "Low_Income_Income_1996_CT.shp"
        elif name == duplicated_name("Income_CT", 3):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "GOVERNMENT"]
            newName = "Government_Transfers_Income_1996_CT.shp"
        elif name == duplicated_name("Income_CT", 4):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "AVERAGE_VA"]
            newName = "Dwellings_Average_Value_Income_1996_CT.shp"
        elif name == duplicated_name("Income_CT", 5):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "MAJOR_REPA", "TOTAL_NU4"]
            newName = "Dwellings_Major_Repair_Income_1996_CT.shp"
        elif name == duplicated_name("Income_CT", 6):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "PERIOD_OF_", "PERIOD_OF1", "TOTAL_NU4"]
            newName = "Dwellings_1960Constructions_Income_1996_CT.shp"
        elif name == duplicated_name("Labour_Occupation_Education_CT", 0):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "UNEMPLOYED", "IN_THE_LAB"]
            newName = "Labour_Unemployment_Labour_1996_CT.shp"
        elif name == duplicated_name("Language_Immigration_Citizenships_CT", 0):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "1991_1996_", "TOTAL_POPU"]
            newName = "Immigration_Language_1996_CT.shp"
        elif name == duplicated_name("Language_Immigration_Citizenships_CT", 1):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "NEITHER_EN", "TOTAL_PO3"]
            newName = "Neither_Language_Language_1996_CT.shp"
        elif name == duplicated_name("Language_Immigration_Citizenships_CT", 2):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "CANADIAN_C", "TOTAL_POPU"]
            newName = "Canadian_Citizen_Language_1996_CT.shp"
        elif name == duplicated_name("Marital_Families_Households_CT", 0):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "TOTAL_LONE", "TOTAL_NUMB"]
            newName = "Lone_Parent_Marital_1996_CT.shp"
        elif name == duplicated_name("Marital_Families_Households_CT", 1):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "LIVING_AL1", "TOTAL_NU3"]
            newName = "Living_Alone_Marital_1996_CT.shp"
        elif name == duplicated_name("Marital_Families_Households_CT", 2):
            keepFields = ["FID", "Shape", "GEOGRAPHY", "MOVABLE_DW", "TOTAL_NU4"]
            newName = "Dwellings_Mobile_Marital_1996_CT.shp"
        else:
            continue

        # Import delete_rename function only once instead execute in every lower tier if condition
        data_process(path, keepFields, newName)

if __name__ == '__main__':
    fcs_in_workspace(inputFolder)
