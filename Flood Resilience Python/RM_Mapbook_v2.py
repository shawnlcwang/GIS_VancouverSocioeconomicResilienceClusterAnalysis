# Import system modules
import arcpy
import os
import sys

arcpy.env.overwriteOutput = True

# Define map layer input folder
inputFolder = "D:\\MSc Thesis\\CCAR Database\\1991\\Mapping_1991_Input"
outputFolder = "D:\\MSc Thesis\\CCAR Database\\1991\\Mapping_1991_Output"

# Define lyr_in_workspace using variable of InputFolder
def lyr_in_workspace(inputFolder):
    # Direct to output folder
    if not os.path.exists(outputFolder):
        os.makedirs(outputFolder)
    # Loop for root, subFolders, files in inputFolder
    for root, subFolders, files in os.walk(inputFolder):
        # define environment workspace
        arcpy.env.workspace = root
        # List feature classes
        fcList = arcpy.ListFeatureClasses(files)
        # Loop for fc in fcList
        for fc in fcList:
            lyrFile = arcpy.Describe(fc).name.strip(".shp") + "_lyr"
            lyrPath = os.path.join(outputFolder, lyrFile + ".lyr")
            arcpy.MakeFeatureLayer_management(fc, lyrFile)
            arcpy.SaveToLayerFile_management(lyrFile, lyrPath, "ABSOLUTE")
            addLayer = arcpy.mapping.Layer(lyrPath)
            arcpy.mapping.AddLayer(df, addLayer, "BOTTOM")

# Read the map document
# mapDoc = arcpy.mapping.MapDocument("CURRENT")
mapDoc = arcpy.mapping.MapDocument("D:\\MSc Thesis\\Flood Resilience Metrics\\Flood Resilience Mapping.mxd")

# List & modify data frames
dfList = arcpy.mapping.ListDataFrames(mapDoc)

# Spatial reference of a specific file
boundaryData = "D:\\MSc Thesis\\CCAR Database\\1991\\CMA\\CMA_EA.shp"
spatialRef = arcpy.Describe(boundaryData).spatialReference

# For loop in dfList
for df in dfList:
    # Set the spatial reference
    df.spatialReference = spatialRef
    # Set the scale of data frame to 1:500,000
    df.scale = 500000
    # Zoom to extent of all the selected features in a data frame
    # If no features are selected, zoom to the full extent of all layers
    df.zoomToSelectedFeatures()
    # Import lyr_in_workspace function to convert feature class into layer files and add to map document
    lyr_in_workspace(inputFolder)
    # List & modify specific layers
    lyrList = arcpy.mapping.ListLayers(mapDoc)
    # For loop in lyrList
    for lyr in lyrList:
        # lyrFile = arcpy.Describe(lyr).name.strip(".shp")
        # lyrPath = os.path.join(outputFolder, lyrFile + ".lyr")
        # fields = arcpy.ListFields(lyrPath)
        # Update layer symbology
        sourceLayer = arcpy.mapping.Layer("D:\\MSc Thesis\\CCAR Database\\1991\\Mapping_1991_Input\\Source_Layer_Properties_1991_EA.lyr")
        arcpy.mapping.UpdateLayer(df, lyr, sourceLayer, True)
        if lyr.symbologyType == "GRADUATED_COLORS":
          lyr.symbology.valueField = "%s" % "Z_score"
          lyr.symbology.numClasses = 5
          lyr.symbology.classBreakValues = [-10, -1.5, -0.5, 0.5, 1.5, 10]
          lyr.symbology.classBreakLabels = ["Low", "Medium-Low", "Medium", "Medium-High", "High"]

        # List & modify specific page layout elements
        elemList = arcpy.mapping.ListLayoutElements(mapDoc)
        for elem in elemList:
            print elem.name

        # List & modify title page layout element
        title = arcpy.mapping.ListLayoutElements(mapDoc, "TEXT_ELEMENT")[0]
        title.elementPositionX = 4.0
        title.elementPositionY = 10.0
        title.fontSize = 24
        if title.text == "Title":
            title.text = "%s" % lyr

        # List & modify legend page layout element (only legend has update item method)
        styleItem = arcpy.mapping.ListStyleItems("USER_STYLE", "Legend Items")[0]
        legend = arcpy.mapping.ListLayoutElements(mapDoc, "LEGEND_ELEMENT")[0]
        # For loop in legend list to remove legend items
        for item in legend.listLegendItemLayers():
            if item.name == "Dwellings_Average_Value_Dwellings_1991_DA_lyr":
                legend.removeItem(item)
            if item.name == "Household_Average_Income_Income_1991_DA_lyr":
                legend.removeItem(item)
            if item.name == "Lone_Parent_Dwellings_1991_DA_lyr":
                legend.removeItem(item)
        legend.updateItem(lyr, styleItem)
        legend.elementPositionX = 1.3
        legend.elementPositionY = 3.5

        # if legend.isOverFlowing:
        #     legend.elementHeight = legend.elementHeight + 0.1

        # List & modify north arrow page layout element
        northArrow = arcpy.mapping.ListLayoutElements(mapDoc, "MAPSURROUND_ELEMENT")[0]
        northArrow.elementPositionX = 7
        northArrow.elementPositionY = 10

        # List & modify scale bar page layout element
        scaleBar = arcpy.mapping.ListLayoutElements(mapDoc, "MAPSURROUND_ELEMENT")[0]
        scaleBar.elementPositionX = 4.2
        scaleBar.elementPositionY = 1.5
        scaleBar.width = 3

        # # Create grid graticule page layout element
        # # Check out Production Mapping extension
        # arcpy.CheckOutExtension('foundation')
        # # Set the values of the tool's parameters using the grid XML files located under the GridTemplates directory
        # ArcDir = arcpy.GetInstallInfo()['InstallDir']
        # grid_xml = ArcDir + "/GridTemplates/Quad_24K_NAD83.xml"
        # # Create grid object from a XML file
        # arcpyproduction.mapping.Grid(ArcDir + r"\GridTemplates\Calibrated_1500K_to_2250K_WGS84.xml")
        # # Check in extension
        # arcpy.CheckInExtension("foundation")
    del lyrList

# Manually update the map document properties modification
arcpy.RefreshActiveView()
arcpy.RefreshTOC()

# Save the map document properties modification
mapDoc.save()

# Exporting PDF Map Books
# pdfPath = "D:\\MSc Thesis\\CCAR Database\\1991\\Mapping_1991_Output\\MapBook_1991_EA.pdf"
# pdfDoc = arcpy.mapping.PDFDocumentCreate(pdfPath)
# mapDoc.dataDrivenPages.exportToPDF("D:\\MSc Thesis\\CCAR Database\\1991\\Mapping_1991_Output\\Maps_1991_EA.pdf")
# pdfDoc.appendPages("D:\\MSc Thesis\\CCAR Database\\1991\\Mapping_1991_Output\\Cover_1991_EA.pdf")
# pdfDoc.appendPages("D:\\MSc Thesis\\CCAR Database\\1991\\Mapping_1991_Output\\Maps_1991_EA.pdf")
# pdfDoc.saveAndClose()

# Remove the reference to the map document
del mapDoc

if __name__ == '__main__':
    inputFolder