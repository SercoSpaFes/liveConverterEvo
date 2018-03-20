
import os,sys,time
import traceback

#
my_PYTHONPATH='.:/home/gilles/shared/CONVERTERS_REFERENCE/luigi/:/home/gilles/shared/CONVERTERS_REFERENCE/glpkg_converter/:/home/gilles/shared/CONVERTERS_REFERENCE/glpkg_converter/eoSip_converter/esaProducts/definitions_EoSip/v101'
my_GDAL_DATA='/home/gilles/anaconda/pkgs/gdal-1.11.2-np19py27_1/share/gdal/'

#
debud = True

def setConverterEnvironment():
    try:
        for item in my_PYTHONPATH.split(':'):
            sys.path.append(item)
        os.environ['PYTHONPATH']=my_PYTHONPATH
        os.environ['GDAL_DATA']=my_GDAL_DATA
    except:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print("converterSetenv error: %s %s" % (exc_type, exc_obj))
        traceback.print_exc(file=sys.stdout)


