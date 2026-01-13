import glob
import time
import pandas as pd
import sqlite3
import os
import numpy as np
import geopandas as gpd
import datetime
from django.template.loader import render_to_string
from shapely import Point, LineString
from core.models import Project
from .models import SPSRevision


class ProjectPreplot(object):
    def __init__(self,path):
        self.dbpath = path


class CommonPreplot(object):
    def __init__(self,prj_id,Config,preplot_type = 1):
        """ Init class for receiver preplot prj_id is project identification and Config is SPS configuration preplot_type is 1 for source and 2 for receivers
         """
        self.prj_id = prj_id
        self.dbase = 'data/' + str(self.prj_id) + '/sps.db'

        self.preplot_type = preplot_type
        self.db = sqlite3.connect(self.dbase)
        self.cursor = self.db.cursor()
        self.prj = Project.objects.filter(id=self.prj_id).get()

        if Config:
            self.pos = SPSRevision.objects.filter(RevName=Config).get()
        else:
            self.pos = SPSRevision.objects.filter(id=0).get()
        if preplot_type == 1: #Source
           self.line_table_name ='SLPreplot'
           self.point_table_name = 'SPPreplot'
           self.list_id ='s'
        elif preplot_type == 2: #Receivers
            self.line_table_name ='RLPreplot'
            self.point_table_name = 'RPPreplot'
            self.list_id ='r'


        self.create_project_vessels_table()
    def create_project_vessels_table(self):
        sql="""
                CREATE TABLE IF NOT EXISTS 
                       project_vessels (
                           ID INTEGER PRIMARY KEY AUTOINCREMENT,
                           Name TEXT NOT NULL UNIQUE,
                           Description TEXT,
                           IMONum TEXT NOT NULL UNIQUE
                       );
            """
        try:
            self.cursor.executescript(sql)
            self.db.commit()
        except Exception as e:
            print(f"Function create_project_vessels_table occurred an error: {e}")
            return {"error":f"Function create_project_vessels_table occurred an error: {e}"}
        print("Table crated project_vessels successfully")
        return {"success":"Table crated project_vessels successfully"}
    def calculate_length(self,x1, y1, x2, y2):
        x_diff = x2 - x1
        y_diff = y2 - y1
        return np.sqrt(x_diff ** 2 + y_diff ** 2)
    def load_from_sps(self,FileName,LineBearing,Tier=1,is_replace =True,keep_duplicates=3):# keep all duplicates
        """Add SPS file with FileName into project dfbase need to setup LineBearing (default 0) and Tier (default 1)
        Parameter isReplace (default True) (true: replace df in bd false: ignore new df)"""
        if self.preplot_type == 2:
            mask = self.prj.RLineMask
        elif self.preplot_type == 1:
            mask = self.prj.SLineMask
        line_pos = [mask.index('L'), mask.rfind('L')] if 'L' in mask else [0, 0]  # Get number of digits for Line Number
        point_pos = [mask.index('P'), mask.rfind('P')] if 'P' in mask else [0, 0]
        db = sqlite3.connect(self.dbase)
        cursor = db.cursor()
        cursor.execute("PRAGMA foreign_keys = OFF")
        start_job_time = time.time()
        dbfilename = os.path.basename(FileName)
        cursor.execute("SELECT COUNT(*) FROM Files WHERE FileName=?", (dbfilename,))
        count = cursor.fetchone()[0]
        if count == 0:
            # If the dbfilename doesn't exist, add it to the table
            cursor.execute("INSERT INTO Files (FileName) VALUES (?)", (dbfilename,))
            db.commit()
            new_id = cursor.lastrowid
            print(f"{dbfilename} added to the Files table with ID: {new_id}")
        else:
            print(f"{dbfilename} already exists in the Files table.")
            new_id = cursor.execute("SELECT ID FROM Files WHERE FileName=?", (dbfilename,)).fetchone()[0]
        Encoding = self.prj.detect_encoding(FileName)
        with open(FileName, 'rt', encoding=Encoding) as f:
            lines = f.readlines()
            i = sum(1 for line in lines if line.startswith('H'))
        cols = ['Line', 'Point', 'PointIndex','X', 'Y','Z']
        dtypes = {'Line': int, 'Point': int, 'X': float,'Y': float,'Z': float}
        colspec = [
                    (self.pos.Line_Start, self.pos.Line_End),
                    (self.pos.Point_Start, self.pos.Point_End),
                    (self.pos.PointIdx_Start, self.pos.PointIdx_End),
                    (self.pos.Easting_Start, self.pos.Easting_End),
                    (self.pos.Northing_Start, self.pos.Northing_End),
                    (self.pos.PointDepth_Start, self.pos.PointDepth_End)
                  ]
        try:
            df = pd.read_fwf(filepath_or_buffer=FileName, skiprows=i,
                               colspecs=colspec, header=None,names=cols, encoding=Encoding,dtype=dtypes)
        except Exception as e:
            print(e)
            return {'error':e}
        df['Tier'] = Tier
        df['Line_FK']=0
        df['File_FK'] = new_id
        df['LineBearing'] = LineBearing
        df['PointIndex'].fillna(1, inplace=True)
        
        num_digits = point_pos[1] - point_pos[0]
        scalar= int("1"+"0"*(num_digits+1))
        df['LinePoint'] = df['Line'] * scalar+df['Point'].astype('int64')
        num_digits2 = len(str(abs(df['LinePoint'].max())))
        scalar2 = int("1" + "0" * (num_digits2 + 1))
        df['TLinePoint'] = df['Tier'] * scalar2+df['LinePoint']
        num_digits3 = line_pos[1] - line_pos[0]
        scalar3 = int("1" + "0" * (num_digits3 + 1))
        df['TierLine'] = df['Tier'] * scalar3+df['Line']
        df['LinePointIndex'] = df['LinePoint']*10+df['PointIndex'].astype("int64")
        # Get unique pairs of values from df
        unique_pairs = zip(df['TierLine'].unique(), df['Line'].unique())
        # Generate the list of values to insert or replace
        # Group by 'TierLine' and calculate various aggregations
        result = df.groupby(['TierLine', 'Line','File_FK']).agg({
            'Point': ['min', 'max', 'count'],  # Add point count per TierLine
            'X': ['first', 'last'],
            'Y': ['first', 'last'],
        })

        # Calculate the length of the line for each 'TierLine'
        result[('Length', 'Line')] = result.apply(lambda row: self.calculate_length(row[('X', 'first')],
                                                                               row[('Y', 'first')],
                                                                               row[('X', 'last')],
                                                                               row[('Y', 'last')]), axis=1)

        result.columns = [f"{level1}_{level2}" if level2 != '' else level1 for level1, level2 in result.columns]

        # Reset index to make 'TierLine' and 'Line' as regular columns
        result.reset_index(inplace=True)
        values = [tuple(row) for row in result.to_numpy()]

        # Execute the INSERT OR REPLACE statement
        cursor.executemany(f"INSERT OR REPLACE INTO {self.line_table_name} (TierLine, Line, File_FK, FirstPoint, LastPoint, Points,StartX,EndX,StartY,EndY,LineLength) VALUES (?,?,?,?,?,?,?,?,?,?,?)", values)
        db.commit()
        try:
            Linedf = pd.read_sql(f"select TierLine,ID FROM {self.line_table_name}",db)

        except Exception as e:
            print(e)
            return {'error':e}
        for line, id in zip(Linedf['TierLine'].tolist(), Linedf['ID'].tolist()):
            df.loc[df['TierLine'] == line, 'Line_FK'] = id
        values = [tuple(row) for row in df.to_numpy()]
        columns_name = df.columns
        questions =",".join(["?" for _ in range(len(columns_name))])
        cn =",".join(columns_name)
        if is_replace:
           cursor.executemany(f"""INSERT OR REPLACE INTO {self.point_table_name} ({cn})
           VALUES({questions})""",values)
        else:
            cursor.executemany(f"""INSERT OR IGNORE INTO {self.point_table_name} ({cn}) VALUES({questions})""", values)
        db.commit()
        cursor.execute("PRAGMA foreign_keys = ON")
        db.close()
        end_job_time = time.time()
        elapsed_job_time = end_job_time - start_job_time
        print("Function took {:.2f} seconds to execute.".format(elapsed_job_time))
        return {'success':f'Uploaded: {df.__len__()} records took %.1f seconds.'%elapsed_job_time}

    def delete_lines(self,linelist):
        db = sqlite3.connect(self.dbase)
        cursor = db.cursor()
        start_job_time = time.time()
        if linelist is not None:
            try:
                cursor.execute("PRAGMA foreign_keys = ON")
                del_list = [(l,) for l in linelist]
                sql = f"DELETE FROM {self.line_table_name} WHERE TierLine = ?"
                cursor.executemany(sql, del_list)
                db.commit()
                end_job_time = time.time()
                elapsed_job_time = end_job_time - start_job_time
                print("Function took {:.2f} seconds to execute.".format(elapsed_job_time))
                return {'success_message': f'Deleted: {len(linelist)} records took %.1f seconds' % elapsed_job_time}
            except sqlite3.IntegrityError as e:
                db.rollback()
                return {"error_message": str(e)}
            finally:
                cursor.execute("PRAGMA foreign_keys = OFF")
                db.close()
        else:
            db.close()
            return {"error_message": 'No lines selected for delete'}

    def delete_line(self,tierline):
        start_job_time = time.time()
        db = sqlite3.connect(self.dbase)
        cursor = db.cursor()
        try:
            cursor.execute(f'DELETE FROM  {self.line_table_name} WHERE TierLine=?',(tierline,))
        except Exception as e:
               print(e)
               db.close()
               return {'error':e}
        db.close()
        end_job_time = time.time()
        elapsed_job_time = end_job_time - start_job_time
        print("Function took {:.2f} seconds to execute.".format(elapsed_job_time))
        return {'success':f'Source Line: {tierline} successfully deleted'}

    def export_sol_and_eol_to_csv(self):
        """Export source line information to csv file."""
        start_job_time = time.time()
        db = sqlite3.connect(self.dbase)
        cursor = db.cursor()
        try:
           df=pd.read_sql(f"select * from {self.line_table_name} ORDER BY TierLIne",db)
        except Exception as e:
               db.close()
               print(e)
               return {'error':e}
        try:
            if self.preplot_type==1:
               df.to_csv(f"{self.prj.QGISOutput}/SL_SOL_EOL.csv",index=False)
            else:
                df.to_csv(f"{self.prj.QGISOutput}/RL_SOL_EOL.csv", index=False)
        except Exception as e:
               print(e)
               db.close()
               return {'error':e}
        db.close()
        end_job_time = time.time()
        elapsed_job_time = end_job_time - start_job_time
        print("Function took {:.2f} seconds to execute.".format(elapsed_job_time))
        return {'success': f'SOL and EOL  successfully exported'}
    def export_splited_sps(self):
        """Split preplot df by line and export them as separate files"""
        start_job_time = time.time()
        db = sqlite3.connect(self.dbase)
        cursor = db.cursor()
        try:
            df = pd.read_sql(f"SELECT TierLine, Line, Point, X,Y FROM {self.point_table_name} ORDER BY TierLIne",self.db)
        except Exception as e:
               print(e)
               db.close()
               end_job_time = time.time()
               elapsed_job_time = end_job_time - start_job_time
               print("Function took {:.2f} seconds to execute.".format(elapsed_job_time))
               return {'error':e}
        for tline, line in zip(df['TierLine'].unique(), df['Line'].unique()):
            with open(f"{self.prj.Export_path}/{line}_Preplot.r01", "wt",encoding="utf-8") as out:
                Linedf = df.loc[df['TierLine'] == tline]
                out.writelines([
                    f"S{d.Line}                {d.Point}1S1                {d.X:.1f} {d.Y:.1f}     0         \n"
                    for d in Linedf.itertuples()
                ])
        db.close()
        end_job_time = time.time()
        elapsed_job_time = end_job_time - start_job_time
        print("Function took {:.2f} seconds to execute.".format(elapsed_job_time))
        return {'success': f'Source preplot successfully splited and exported to {self.prj.Export_path}'}
    def export_to_shapes(self,tline ):
        """Export receiver line information to shapes file."""
        start_job_time = time.time()
        db = sqlite3.connect(self.dbase)
        cursor = db.cursor()
        try:
            df=pd.read_sql(f"select TierLine, Line, Point, X,Y from {self.point_table_name} WHERE TierLine = {tline}",db)
        except Exception as e:
               print(e)
               db = sqlite3.connect(self.dbase)
               cursor = db.cursor()
               end_job_time = time.time()
               elapsed_job_time = end_job_time - start_job_time
               print("Function took {:.2f} seconds to execute.".format(elapsed_job_time))
               return {'error':e}
        if df.__len__() <= 1:
            db.close()
            return {'error':f'Line: {tline} has less than 2 Points'}
        line = df['Line'].values[0]
        df['geometry'] = [Point(xy) for xy in zip(df.X, df.Y)]
        df = df.groupby(['TierLine','Line'])['geometry'].apply(lambda x: LineString(x.tolist()))
        geodf = gpd.GeodfFrame(df)
        line_folder = f"{self.prj.Shape_folder}/RL_Shapes_Lines/"
        point_folder = f"{self.prj.Shape_folder}/RL_Shapes_Points/"
        os.makedirs(line_folder, exist_ok=True)
        os.makedirs(point_folder, exist_ok=True)
        geodf.to_file(f"{point_folder}/{line}.shp")
        gpd.GeodfFrame(df, geometry='geometry').to_file(f"{line_folder}/{line}_line.shp")
        db.close()
        end_job_time = time.time()
        elapsed_job_time = end_job_time - start_job_time
        print("Function took {:.2f} seconds to execute.".format(elapsed_job_time))
        return {'success': f'Shape created for line {line} file saved {line_folder} and {point_folder}'}
    def line_list(self):
        """ Get list of lines from RLPreplot"""
        start_job_time = time.time()
        db = sqlite3.connect(self.dbase)
        cursor = db.cursor()
        try:
            df = pd.read_sql(f"SELECt * FROM {self.line_table_name}", db)
        except Exception as e:
            print(e)
            db.close()
            end_job_time = time.time()
            elapsed_job_time = end_job_time - start_job_time
            print("Function took {:.2f} seconds to execute.".format(elapsed_job_time))
            return None
        db.close()
        end_job_time = time.time()
        elapsed_job_time = end_job_time - start_job_time
        print("Function took {:.2f} seconds to execute.".format(elapsed_job_time))
        return render_to_string("preplotpage/rec_line_list.html",{'LineList':df.itertuples(),
                                                                  'list_id':f'{self.list_id}l_list'})
    def point_list(self,TierLine):
        """ Get list of points from SLPreplot for selected line"""
        start_job_time = time.time()
        db = sqlite3.connect(self.dbase)
        cursor = db.cursor()
        try:
            df = pd.read_sql(f"SELECt * FROM {self.point_table_name} WHERE TierLine = {TierLine}", db)
        except Exception as e:
            print(e)
            db.close()
            end_job_time = time.time()
            elapsed_job_time = end_job_time - start_job_time
            print("Function took {:.2f} seconds to execute.".format(elapsed_job_time))
            return None
        end_job_time = time.time()
        elapsed_job_time = end_job_time - start_job_time
        print("Function took {:.2f} seconds to execute.".format(elapsed_job_time))
        db.close()
        return render_to_string("preplotpage/rec_point_list.html", {'LineList': df.itertuples(),
                                                                    'list_id':f'{self.list_id}p_list'})
    def get_preplot_summary(self):
        try:
            df = pd.read_sql("SELECT * FROM PREPLOT_SUMMARY",self.db)
        except Exception as e:
            print(e)
            return {"error":e}
        if df.__len__() == 0:
           return {"error":f'Preplot summary table has no df'}
        summary = df.to_dict(orient='records')[0]
        html = render_to_string("preplotpage/preplot_summary.html",{'PPSummary':summary})
        return html

    def get_preplot_statistics(self):
        start_job_time = time.time()
        db = sqlite3.connect(self.dbase)
        cursor = db.cursor()
        sql=f"SELECT COUNT(DISTINCT(TierLine)) as Lines, sum(Points) as Points, sum(LineLength) as TotalLength FROM {self.line_table_name}"
        try:
            Lines,Points,TotalLength=db.execute(sql).fetchone()
        except Exception as e:
               db.close()
               print(e)
               end_job_time = time.time()
               elapsed_job_time = end_job_time - start_job_time
               print("Function took {:.2f} seconds to execute.".format(elapsed_job_time))
               return {'error_message':e}
        db.close()
        end_job_time = time.time()
        elapsed_job_time = end_job_time - start_job_time
        print("Function took {:.2f} seconds to execute.".format(elapsed_job_time))
        return {'Lines':Lines,'Points':Points,'TotalLength':TotalLength}
    def get_shape_files(self):
        start_job_time = time.time()
        file_list = [os.path.basename(f) for f in glob.glob(f"{self.prj.Shape_folder}/*.shp")]
        html = render_to_string("preplotpage/shape_file_list.html",{'file_list':file_list})
        end_job_time = time.time()
        elapsed_job_time = end_job_time - start_job_time
        print("Function took {:.2f} seconds to execute.".format(elapsed_job_time))
        return html
    def add_vessel_to_project(self,name, imo, description):
        sql=f"INSERT OR REPLACE INTO project_vessels (Name, IMONum, Description) VALUES ('{name}','{imo}','{description}')"
        try:
            self.cursor.execute(sql)
        except Exception as e:
            print(f"function add_vessel_to_project occurred an error:{e}")
            return {'error':f"function add_vessel_to_project occurred an error:{e}"}
        self.db.commit()
        print(f"New vessel:{name},{imo},{description} added to project successfully")
        return {'success':f"New vessel:{name},{imo},{description} added to project successfully"}
    def delete_vessel_from_project(self,name):
        sql=f"DELETE FROM project_vessels WHERE Name='{name}'"
        try:
            self.cursor.execute(sql)
        except Exception as e:
            print(f"function delete_vessel_from_project occurred an error:{e}")
            return {"error":f"function delete_vessel_from_project occurred an error:{e}"}
        else:
            self.db.commit()
            print(f"function delete_vessel_from_project succeeded vessel:{name} was deleted")
            return {'success':f"Deleted vessel:{name} successfully"}
    def get_project_vessels_list(self):
        start_job_time = time.time()
        sql = "SELECT * FROM project_vessels"
        try:
            df = pd.read_sql(sql, self.db)
        except Exception as e:
            print(f"function get_project_vessels occurred an error:{e}")
            return {'error': f"function get_project_vessels occurred an error:{e}"}
        end_job_time = time.time()
        if df.empty:
            print(f"No vessels associated to the current project")
            return {'error': f"No vessels associated to the current project"}
        else:
            return df.itertuples()

    def get_project_vessels(self):
        start_job_time = time.time()
        sql="SELECT * FROM project_vessels"
        try:
            df = pd.read_sql(sql,self.db)
        except Exception as e:
            print(f"function get_project_vessels occurred an error:{e}")
            return {'error':f"function get_project_vessels occurred an error:{e}"}
        end_job_time = time.time()
        if df.empty:
           print(f"No vessels associated to the current project")
           return {'error':f"No vessels associated to the current project"}
        else:
            html = render_to_string("preplotpage/project_vessels_table.html",{"prj_vessels":df.itertuples()})
            return html




