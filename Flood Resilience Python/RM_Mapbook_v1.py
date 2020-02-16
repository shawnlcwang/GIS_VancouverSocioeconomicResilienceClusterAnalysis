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
        sourceLayer = arcpy.mapping.Layer("D:\\MSc Thesis\\CCAR Database\\1991\\Mapping_1991_Input\\Source_Layer_Properties_1991_EA.lyr")
        arcpy.mapping.UpdateLayer(df, lyr, sourceLayer, True)
        if lyr.symbologyType == "GRADUATED_COLORS":
          lyr.symbology.valueField = "%s" % "Z_score"
          lyr.symbology.numClasses = 5
          lyr.symbology.classBreakValues = [-10, -1.5, -0.5, 0.5, 1.5, 10]
          lyr.symbology.classBreakLabels = ["Low", "Medium-Low", "Medium", "Medium-High", "High"]
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