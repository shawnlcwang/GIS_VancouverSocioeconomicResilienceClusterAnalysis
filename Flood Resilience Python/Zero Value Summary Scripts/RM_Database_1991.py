
# Import system modules
import arcpy
import os
import sys

arcpy.env.overwriteOutput = True

# Read the parameter values
# inputFolder = arcpy.GetParameterAsText(0)
# outputFolder = arcpy.GetParameterAsText(1)
inputFolder = "D:\\MSc Thesis\\CCAR Database\\1991\\Social_Resilience_1991_Input"
outputFolder = "D:\\MSc Thesis\\CCAR Database\\Missing_Values\\1991"

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
        # Pre-declare variables of keepFields & newName with empty container for delete_rename function
        # Allow to let the lower tier block of different keepFields & newName codes
        # Fill in the upper tier keepFields & newName
        keepFields = []
        newName = ""

        # Import duplicated_name function to define names for if conditions
        if name == duplicated_name("Dwelling_households_EA", 0):
            # Define keepField & newName parameters for delete_rename function
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL6", "COL8"]
            newName = "Dwellings_Rental_Dwellings_1991_EA.shp"
        elif name == duplicated_name("Dwelling_households_EA", 1):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL6", "COL17"]
            newName = "Dwellings_Mobile_Dwellings_1991_EA.shp"
        elif name == duplicated_name("Dwelling_households_B_EA", 0):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL9"]
            newName = "Dwellings_Average_Value_Dwellings_1991_EA.shp"
        elif name == duplicated_name("Dwelling_households_B_EA", 1):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL27", "COL34"]
            newName = "Lone_Parent_Dwellings_1991_EA.shp"
        elif name == duplicated_name("Dwelling_households_B_EA", 2):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL6", "COL12"]
            newName = "Dwellings_Major_Repair_Dwellings_1991_EA.shp"
        elif name == duplicated_name("Dwelling_households_B_EA", 3):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL6", "COL13", "COL14"]
            newName = "Dwellings_1960Construction_Dwellings_1991_EA.shp"
        elif name == duplicated_name("Families_EA", 0):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL47", "COL51"]
            newName = "Living_Alone_Families_1991_EA.shp"
        elif name == duplicated_name("Income_EA", 0):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL60"]
            newName = "Household_Average_Income_Income_1991_EA.shp"
        elif name == duplicated_name("Income_EA", 1):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL54"]
            newName = "Low_Income_Income_1991_EA.shp"
        elif name == duplicated_name("Income_EA", 2):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL20"]
            newName = "Government_Transfers_Income_1991_EA.shp"
        elif name == duplicated_name("Labor_activity_B_EA", 0):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL7", "COL9"]
            newName = "Labour_Unemployment_Labour_1991_EA.shp"
        elif name == duplicated_name("Language_B_EA_", 0):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL6", "COL45"]
            newName = "Neither_Language_Language_1991_EA.shp"
        elif name == duplicated_name("Reli_ethnic_ori_citzn_immi_B_EA_", 0):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL6", "COL87"]
            newName = "Immigration_Religion_1991_EA.shp"
        elif name == duplicated_name("Reli_ethnic_ori_citzn_immi_B_EA_", 1):
            keepFields = ["FID", "Shape", "EA", "EA_STR", "OID_", "COL6", "COL42"]
            newName = "Canadian_Citizen_Religion_1991_EA.shp"

        elif name == duplicated_name("Citizen_Immigration_Migration_CT", 0):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL4", "COL63", "COL4_", "COL63_"]
            field_process1(path, "COL4_", "COL63_", "!COL4!", "!COL63!")
            newName = "Immigration_Citizen_1991_CT.shp"
        elif name == duplicated_name("Citizen_Immigration_Migration_CT", 1):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL7", "COL42", "COL7_", "COL42_"]
            field_process1(path, "COL7_", "COL42_", "!COL7!", "!COL42!")
            newName = "Canadian_Citizen_Citizen_1991_CT.shp"
        elif name == duplicated_name("Education_income_CT", 0):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL160"]
            newName = "Household_Average_Income_Education_1991_CT.shp"
        elif name == duplicated_name("Education_income_CT", 1):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL149"]
            newName = "Low_Income_Education_1991_CT.shp"
        elif name == duplicated_name("Education_income_CT", 2):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL95"]
            newName = "Government_Transfers_Education_1991_CT.shp"
        elif name == duplicated_name("Labour_Employment_Occupation_CT", 0):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL43", "COL45", "COL43_", "COL45_"]
            field_process1(path, "COL43_", "COL45_", "!COL43!", "!COL45!")
            newName = "Labour_Unemployment_Labour_1991_CT.shp"
        elif name == duplicated_name("Language_CT", 0):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL4", "COL117", "COL4_", "COL117_"]
            field_process1(path, "COL4_", "COL117_", "!COL4!", "!COL117!")
            newName = "Neither_Language_Language_1991_CT.shp"
        elif name == duplicated_name("Marital_Families_Households_Dwellings_CT", 0):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL47", "COL49", "COL47_", "COL49_"]
            field_process1(path, "COL47_", "COL49_", "!COL47!", "!COL49!")
            newName = "Dwellings_Rental_Marital_1991_CT.shp"
        elif name == duplicated_name("Marital_Families_Households_Dwellings_CT", 1):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL141", "COL141_"]
            field_process0(path, "COL141_", "!COL141!")
            newName = "Dwellings_Average_Value_Marital_1991_CT.shp"
        elif name == duplicated_name("Marital_Families_Households_Dwellings_CT", 2):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL70", "COL88", "COL70_", "COL88_"]
            field_process1(path, "COL70_", "COL88_", "!COL70!", "!COL88!")
            newName = "Lone_Parent_Marital_1991_CT.shp"
        elif name == duplicated_name("Marital_Families_Households_Dwellings_CT", 3):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL111", "COL115", "COL111_", "COL115_"]
            field_process1(path, "COL111_", "COL115_", "!COL111!", "!COL115!")
            newName = "Living_Alone_Marital_1991_CT.shp"
        elif name == duplicated_name("Marital_Families_Households_Dwellings_CT", 4):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL138", "COL144", "COL138_", "COL144_"]
            field_process1(path, "COL138_", "COL144_", "!COL138!", "!COL144!")
            newName = "Dwellings_Major_Repair_Marital_1991_CT.shp"
        elif name == duplicated_name("Marital_Families_Households_Dwellings_CT", 5):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL47", "COL58", "COL47_", "COL58_"]
            field_process1(path, "COL47_", "COL58_", "!COL47!", "!COL58!")
            newName = "Dwellings_Mobile_Marital_1991_CT.shp"
        elif name == duplicated_name("Marital_Families_Households_Dwellings_CT", 6):
            keepFields = ["FID", "Shape", "AREA", "PERIMETER", "G91CT0_", "G91CT0_ID", "CA", "CT_NAME", "PROV", "CA_CTNAME", "OID_", "COL138", "COL145", "COL146", "COL138_", "COL145_", "COL146_"]
            field_process2(path, "COL138_", "COL145_", "COL146_", "!COL138!", "!COL145!", "!COL146!")
            newName = "Dwellings_1960Constructions_Marital_1991_CT.shp"
        else:
            continue

        # Import delete_rename function only once instead execute in every lower tier if condition
        data_process(path, keepFields, newName)

if __name__ == '__main__':
    fcs_in_workspace(inputFolder)
