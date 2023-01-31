import os
import sys
import xarray as xr
import pandas as pd
from gridtools import grid_from_rc
from gridtools import Grid

'''
This is a collection of wrapper scripts to call the Climate Data Operator (cdo) to achieve common
tasks on geolocated netcdf files.

ensureCorrectGrid() ensures that the netcdf file provided is on the user-provided lat/lon grid and if not
                                creates it and stores it in a predefined location for future use.
ensureReportedTimeIsStartOfMeasurmentInterval() checks whether the first time dimension value is one (undesired)
                             or zero (as it should). Normally the netcdf header should provide whether the times reported
                             refer to the start of the time step, the middle or the end. VPRM files do, EDGAR4.3 files don't.
                             Hence we cannot rely on the header info and we use a primitive and brutal approach with zero
                             elegance: if the first time step starts at one, then measurments are assumed to be reported at the
                             end of each time step, while Lumia expect this time to represent the beginning of the time interval.
                             Therefor we the time axis back in time by one timestep which is then equivalent to having
                             times representing the beginning of an observation period.
'''

def ensureCorrectGrid(sExistingFile,  grid: Grid = None):
    '''
    Function ensureCorrectGrid
    interpolate the spatial coordinates in sExistingFile if necessary

    We trust the climate data operator software CDO from Max Planck Hamburg to do a decent job on interpolation - and it is easy to use
    CDO Homepaqe:  https://code.mpimet.mpg.de/projects/cdo
    First we need the reference grid onto which to map. For this we extract it from any existing ICOS flux file with the correct grid, e.g.
    cdo griddes flux_co2.VPRM.2018.nc >cdo-icos-quarter-degree.grid
    Then we call cdo with the conservative remapping command (see below and study the cdo user guide).
    We also double checked the output to make sure it was mapped correctly onto the provided grid.

        - if the resolution in sExistingFile is the desired one, then just return sExistingFile as the file name
        - use the existing matched grid file in case cdo has been called previously for the requested input file (see below) and return the name of that file
        - else call cdo to interpolate to the user requested lat/lon grid, save its output to a predetermined location and put the dLat-dLon into the file extension and hand that output file back
    @param sExistingFile an existing netcdf data file like a co2 flux file or other with a lat/lon grid that cdo understands
    @type string
    @param grid required parameter that defines the extent and spatial resolution of the lat/lon rectangle requested (defaults to None)
    @type Grid (optional)
    '''
    tim0=None
    if(grid is None) or (sExistingFile is None) :
        print("Fatal error in xr.ensureCorrectGrid(): no grid provided or no existing file provided.")
        sys.exit(1)
    # step 1: check if a file with the right spatial resolution already exist. If yes, return that file name and we are done
    # grid may look something like Grid(lon0=-15, lon1=35, lat0=33, lat1=73, dlon=0.25, dlat=0.25, nlon=200, nlat=160)
    # create the file name extension: lat and lon in degrees*1000
    sdlat=str(int(grid.dlat*1000))
    sdlon=str(int(grid.dlon*1000))
    fnameOut="."+os.path.sep+"regridded"+os.path.sep+sdlat+'x'+sdlon+os.path.sep+sExistingFile.split(os.path.sep)[-1] +".dLat"+sdlat+"dLon"+sdlon
    print('Hunting for flux input file '+fnameOut,  flush=True)
    try:
        # Have we created this file previously so we could simply read it instead of creating it first?
        f=open(fnameOut, 'rb')
        f.close()
        # tim0=0  # we could assume time axis starts at zero. If this software created it, then that should be the case. But let's be prudent....
    except:
        # No drama. We only need to create an interpolated version of the existing file
        # step 2: figure out the grid of the existing file sExistingFile
        # #   ncdump -h sExistingFile    or     cdo griddes sExistingFile
        # TODO: we cannot hard-wire the the name of the variable(s) to drop to "NEE" - either figure out how to read only the dimensions or how to
        # determine the name(s) of the reported variable(s) so we can drop it/them
        xrExisting = xr.open_dataset(sExistingFile, drop_variables='NEE') # only read the dimensions + 'emission'
        fLats=xrExisting.lat
        fLons=xrExisting.lon
        dTime=xrExisting.time.data
        t1 = pd.Timestamp(dTime[0])
        tim0=t1.hour
        d=dict(xrExisting.dims) # contains the shape of the existing file as {'lat':480, 'lon':400, 'time':8760}
        # print(fLats.values[d['lat'] - 1],  flush=True)
        LatWidth=fLats.values[d['lat'] - 1] - fLats.values[0]    # north-south-extent of the stored region in degrees latitude
        dLatExs=abs(LatWidth/(d['lat'] -1))                                      # stepsize or difference between nearest grid-points in degrees latitude
        LonWidth=fLons.values[d['lon'] - 1] - fLons.values[0]  # width/east-west extent of the stored region in degrees longitude
        dLonExs=abs(LonWidth/(d['lon'] - 1))                                    # stepsize or difference between nearest grid-points in degrees longitude

        # step 3: Then compare the two grids, that is to say the desired grid and the one extracted from the existing file
        if ((abs(grid.dlat - dLatExs) < 0.002) and (abs(grid.dlon - dLonExs) < 0.002)):
            if ((grid.nlat==d['lat']) and (grid.nlon==d['lon'])):
                if ((abs((grid.lat0+0.5*grid.dlat) - fLats.values[0]) < 0.01) and (abs((grid.lon0+0.5*grid.dlon) - fLons.values[0]) < 0.01)):
                    return(sExistingFile, tim0)  # The original file already matches the user-requested grid. Thus, just hand that name back.
        # step 4: call cdo and write the interpolated output file into pre-determined hierarchies and append an extension to the PID based on spatial resolution aka
        #             unique output file name. Upon success, the new file name is then returned by this function.
        # Example for calling cdo: cdo remapcon,cdo-icos-quarter-degree.grid  /data/dataAppStorage/netcdf/xLjxG3d9euFZ9SOUj69okhaU ./250/xLjxG3d9euFZ9SOUj69okhaU.dLat250dLon250
        fRefGridFile="."+os.path.sep+"regridded"+os.path.sep+sdlat+'x'+sdlon+os.path.sep+'cdo-icos-'+"dLat"+sdlat+"dLon"+sdlon+'-reference.grid'
        try:
            # Have we created this file previously so we could simply read it instead of creating it first?
            f=open(fRefGridFile, 'rb')
            f.close()
        except:
            print('Fatal error: Cannot find the grid file '+fRefGridFile+' below your working folder. Either copy it there or create the file with')
            print('cdo griddes YOUR_ANY_NETCDFFILE_ON_DESIRED_GRID >'+fRefGridFile)
            print('Next time Lumia automatically creates the regridded data file by executing:')
            print('cdo remapcon,'+fRefGridFile+' '+sExistingFile+'  '+"."+os.path.sep+"regridded"+os.path.sep+sdlat+'x'+sdlon+os.path.sep+os.path.basename(sExistingFile)+'.'+"dLat"+sdlat+"dLon"+sdlon, flush=True)
            # print('cdo  remapcon,cdo-icos-quarter-degree.grid  /data/dataAppStorage/netcdf/xLjxG3d9euFZ9SOUj69okhaU ./250/xLjxG3d9euFZ9SOUj69okhaU.dLat250dLon250', flush=True)
            sys.exit(-1)
        sCreateOutputDir='mkdir -p regridded'+os.path.sep+sdlat+'x'+sdlon
        os.system(sCreateOutputDir)  # We may need to create the folder as well.
        cdoCmd='cdo remapcon,cdo-icos-quarter-degree.grid  '+sExistingFile+' '+fnameOut
        try:
            # Call CDO in an external subprocess
            # The Eric7 remote debugger does not like the subprocess command and does weird stuff.
            # I got the error: cdo (Abort): Operator missing, /opt/conda/envs/lumia/lib/python3.9/site-packages/eric7/DebugClients/Python/DebugClient.py is a file on disk!
            #     subprocess.run(cdoCmd)
            os.system(cdoCmd)
        except:
            print("Fatal error: Calling cdo failed. Please make sure cdo is installed and working for you. Try running >>"+cdoCmd+"<< yourself in your working directory before running Lumia again.")
            sys.exit(-1)
        try:
            # Did cdo create the re-gridded flux file as expected?
            f=open(fnameOut, 'rb')
            f.close()
        except:
            print("Fatal error: cdo did not create the re-gridded output file "+fnameOut+" as expected from the command >>cdo "+cdoCmd+"<<.")
            sys.exit(-1)
        else:
            return(fnameOut, tim0)
    return(fnameOut, tim0) # we will use a re-gridded file we created earlier or that already exists
    

def ensureReportedTimeIsStartOfMeasurmentInterval(sExistingFile,  tim0, grid: Grid = None):
    '''  
    ensureReportedTimeIsStartOfMeasurmentInterval() checks whether the first time dimension value is one (undesired)
                             or zero (as it should). Normally the netcdf header should provide whether the times reported
                             refer to the start of the time step, the middle or the end. VPRM files do, EDGAR4.3 files don't.
                             Hence we cannot rely on the header info and we use a primitive and brutal approach with zero
                             elegance: if the first time step starts at one, then measurments are assumed to be reported at the
                             end of each time step, while Lumia expect this time to represent the beginning of the time interval.
                             Therefor we the time axis back in time by one timestep which is then equivalent to having
                             times representing the beginning of an observation period.
        # TODO: If Time starts with one rather than zero hours, then the time recorded refers to the end of the 1h measurement interval
        #             as opposed to Lumia, which expects that time to represent the start of the measurement time interval.
        # We can fix this by shifting the time axis by one hour (with cdo):
        # cdo shifttime,-1hour xLjxG3d9euFZ9SOUj69okhaU.dLat250dLon250.eots xLjxG3d9euFZ9SOUj69okhaU.dLat250dLon250
        # TODO: This needs to be made smarter so we can call CDO and fix the time axis no matter what.....
'''
    if((tim0 is not None) and (tim0==0)):
            # we are good. No need to shift. Time dimension starts at zero.
            return(sExistingFile)
    if(grid is None) or (sExistingFile is None) :
        print("Fatal error in xr.ensureCorrectGrid(): no grid provided or no existing file provided.")
        sys.exit(1)
    sdlat=str(int(grid.dlat*1000))
    sdlon=str(int(grid.dlon*1000))
    
    # Do we have to shift the time axis or not? 
    # read the first value from the time dimension and see if it is zero.
    xrExisting = xr.open_dataset(sExistingFile, drop_variables='NEE') # only read the dimensions
    dTime=xrExisting.time.data
    t1 = pd.Timestamp(dTime[0])
    tim0=t1.hour
    # print('tim0=%d'%tim0, flush=True)
    if(tim0==0):
            # we are good. No need to shift. Time dimension starts at zero.
            return(sExistingFile)
    tStep=dTime[1] - dTime[0]
    tStep*=1e-9  # from nanoseconds to seconds - we expect 3600s=1h
    timeStep=int(tStep/3600) # time step in hours
    # print('time step= %dh'%timeStep, flush=True)
    # We expect tim0==tStep at this stage, which is the reason for shifting the time dimension by one time unit.
    if(tim0!=timeStep): 
        print('Warning in Lumia formatter/cdoWrapper.ensureReportedTimeIsStartOfMeasurmentInterval(): First time axis value is not zero(=midnight) or one time step.', flush=True)
    sRenameCmd=''
    fnameOutput='.'+os.path.sep+'regridded'+os.path.sep+sdlat+'x'+sdlon+os.path.sep+os.path.basename(sExistingFile).split(os.path.sep)[-1]
    if('./regridded/' in sExistingFile):
        #  move the existing local file with time representing eots (end of time step) out of the way
        sRenameCmd='mv '+sExistingFile+' '+fnameOutput+'.eots'
    else:
        # it is a file straight from the carbon portal with correct grid to start with, but time alone is shifted.
        sRenameCmd='cp '+sExistingFile+' '+fnameOutput+'.eots'
    os.system(sRenameCmd)  
    print(sRenameCmd, flush=True)
    cdoCmd='cdo shifttime,-%dhour %s.eots  %s'%(timeStep, fnameOutput, fnameOutput)
    print(cdoCmd, flush=True)
    os.system(cdoCmd) 
    return(sExistingFile)
    

