 
import pandas as pd
import numpy as np
from pandas import read_csv,DataFrame,concat,Series
from datetime import datetime,timedelta
from geopy.distance import geodesic
import holidays
from tensorflow.keras.models import load_model
import os
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import LabelEncoder,MinMaxScaler
from sklearn.ensemble import RandomForestRegressor 
import pickle
import warnings
import operator
from collections import defaultdict
 
import pickle
import requests
import warnings
warnings.filterwarnings("ignore")


my_path=os.getcwd()

diry='/'
for i,j in enumerate(my_path.split('/')[1:-1]):
    diry=diry+str(j)
    if i!=len( my_path.split('/')[1:-1] )-1:
        diry=diry+'/'
        

data_path=os.path.join(diry,'Data_Lane_VT_Monthly')
support_file_path=os.path.join(diry,'Transporter_scoring_Combined','SupportFiles','DFS_Files')
processed_datafile_path=os.path.join(diry,'Transporter_scoring_Combined','DataFiles')
model_file_path=os.path.join(diry,'Transporter_scoring_Combined','ModelFiles')
model_out_path=os.path.join(diry,'Transporter_scoring_Combined','OutputFiles','DFS_output')

working_columns=['Quantity',
'Arrival Breached At',
'Vehicle Type',
'contract_start_date',
'contract_end_date',
'Indent ID',
'Source_Depot_City',
'Arrived At',
'Dispatched At',
'sla_delay_charges',
'contract_source',
'depot_lat_long',
'Gross Weight',
'Transit Time',
'Delivery Date',
'Customer',
'contract_destination',
'contract_type',
'Transporter',
'Created Date',
'Base Freight',
'consignee_lat_long',
'Actual Freight',
'distance',
'carton_damage_charges',
'damage_charges',
'consignee_pincode',
'contract_id',
'Destination',
'Indent Type',
'contract_validity',
'Gross Volume',
'shortage_charges']


# In[6]:


class DataCorrectImputeUpdate:   
    
    def data_pull():
        df_history=DataFrame()
        start='2019-01-01'
        updated_time=(datetime.strptime(start,'%Y-%m-%d'))
        now = datetime.now()
        today_date=(pd.to_datetime(now.strftime("%Y-%m-%d %H:%M:%S")))
        last_month=datetime.strptime(str(today_date.year)+'-'+str(today_date.month)+'-01','%Y-%m-%d')

        while updated_time <last_month: #pd.to_datetime(datetime.now().strftime("%Y-%m-%d")):


            if (updated_time.month==1):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(31-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(31-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(31-1)




            if (updated_time.month==2) and (updated_time.year%4==0) :
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(29-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(29-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(29-1)

            if (updated_time.month==2) and (updated_time.year%4!=0) :
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(28-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(28-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(28-1)


            if (updated_time.month==3):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(31-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(31-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(31-1)



            if (updated_time.month==4):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(30-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(30-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(30-1)



            if (updated_time.month==5):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(31-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(31-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(31-1)




            if (updated_time.month==6):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(30-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(30-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(30-1)



            if (updated_time.month==7):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(31-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(31-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(31-1)




            if (updated_time.month==8):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(31-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(31-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(31-1)





            if (updated_time.month==9):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(30-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(30-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(30-1)




            if (updated_time.month==10):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(31-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(31-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(31-1)




            if (updated_time.month==11):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(30-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(30-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(30-1)




            if (updated_time.month==12):
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(updated_time).split(' ')[0],str(updated_time+timedelta(31-1)).split(' ')[0] )
                json_out_file =os.path.join(data_path,'Lane-VT-{}.json'.format(str(updated_time).split(' ')[0]+'to'+str(updated_time+timedelta(31-1)).split(' ')[0] ))

                updated_time=updated_time+timedelta(31-1)
                


            dfj = None

            try:
                dfj = pd.read_json(json_out_file, convert_dates=True)[working_columns]
            except ValueError:
                print(json_out_file)
                try:
                    dataset_url_check= 'http://35.154.192.224:8090/lane_vt_report?from_date=2019-07-01&to_date=2019-07-02'
                    response_check = requests.post(dataset_url_check, data={},timeout=20)
                    response = requests.post(dataset_url, data={})
                    if response.status_code == 200:
                        try:
                            #save Dataset to json
                            file = open(json_out_file, "w")
                            file.write(response.text) 
                            file.close() 
                        except OSError as err:
                            print("OS error: {0}".format(err))

                        #read json into dataframe
                        try:
                            dfj = pd.read_json(json_out_file, convert_dates=True)[working_columns]
                        
                        except ValueError:
                            print ('File not found:', json_out_file)
                            
                except:
                    print('Database Connection Error: Unable To Write New Data')

            
            df_history=df_history.append(dfj)


            updated_time=updated_time+timedelta(1)


        return df_history
        
        

    def Customer_name_correction(df):
        
        CustomerMap = {'Signify Innovations India Limited':'Signify',
                       'Marico Limited':'Marico',
                       'Orient Electric Ltd':'OEL'}
        df['Customer'] = df['Customer'].apply(lambda x: CustomerMap[x] if x in CustomerMap.keys() else x)
        df['Customer'] = df['Customer'].apply(lambda x: x.strip().capitalize())

        return df
    
    def Transporter_name_correction(df):
        df=df.dropna(subset=['Transporter'])
        
        TransMap = {'A.D. ROADLINES (INDIA) REGD.': 'A.D. Roadlines (INDIA) Regd',
                    'ABIZER CARRIERS':'Abizer Carriers',
                    'ANAND ROADLINES':'ANAND ROAD LINES',
                    'Anand Roadlines':'ANAND ROAD LINES',
                    'GATI-KINTETSU EXPRESS PVT LTD':'GATI KINTETSU EXPRESS',
                    'KAPOOR FREIGHT CARRIERS PVT LTD':'Kapoor Freight Carrier Pvt. Ltd',
                    'Om Logistics Limited':'OM Logistics',
                    'Om Logistics Ltd':'OM Logistics',
                    'OTS LIMITED':'OTS Limited',
                    'PANDEY ROAD LINES':'Pandey Roadlines',
                    'RCI LOGISTICS PVT LTD':'RCI Logistics Pvt Ltd',
                    'RIVIGO SERVICES PRIVATE LIMITED':'Rivigo Services Private Limited',
                    'SD CARGO PVT. LTD.':'SD CARGO PVT LTD',
                    'Sri Ramadas Motor Transpo':'Sri Ramadas Motor Transport Limited',
                    'STELLAR INNOVATIVE TRANSPORTATION':'Stellar Innovative Transportation',
                    'WHEELSEYE TECHNOLOGY INDIA PRIVATE LIMITED':'WHEELSEYE TECHNOLOGY INDIA PVT',
                    'ZINKA LOGISTICS SOLUTIONS PRIVATE':'ZINKA LOGISTICS SOLUTIONS',
                    'MIDDLETON LOGISTIC SOLUTIONS':'MIDDLETON LOGISTICS SOLUTIONS',
                    'SPOTON LOGISTICS PRIVATE LIMITED':'Spoton logistics Private Limited',
                    'Sri Ramadas Motor Transpo':'Sri Ramadas Motor Transport Limited',
                    'Sri Ramadas Motor Transport Ltd':'Sri Ramadas Motor Transport Limited'
        }
        
        
        df['Transporter'] = df['Transporter'].apply(lambda x: TransMap[x] if x in TransMap.keys() else x)
        df['Transporter'] = df['Transporter'].apply(lambda x: x.strip().capitalize())

        return df
        
        
    def sourcemap(df):    
        SourceMap = {
        'Guwahati (t)': 'Guwahati',
        'Jaipur-rajasthan': 'Jaipur',
        'Mil sc -ameya food': 'Coimbatore',
        'Mil- guwahati plant' : 'Guwahati',    
        'Mil- jalgaon plants' : 'Jalgaon',
        'Mil- kanjikode plant' : 'Kanjikode',
        'Mil- pondy plants' : 'Puducherry',
        'Tirumala-hyderabad' : 'Tirumala',
        'R598-coimbatore' : 'Coimbatore',
        'Snqz - khopoli' : 'Khopoli',
        'Snrq' : 'Coimbatore',
        'Jaipur-Rajasthan': 'Jaipur'
        }

        df['Source_Depot_City'] = df['Source_Depot_City'].apply(lambda x: x.strip().capitalize())
        df['Source_Depot_City'] = df['Source_Depot_City'].apply(lambda x: x.replace(',', '_'))
        df['Source'] = df['Source_Depot_City'].apply(lambda x: SourceMap[x] if x in SourceMap.keys() else x)
        df=df.drop(['Source_Depot_City'],1)
        return df

    def destmap(df):    
        DestMap = {
        'Balaosre': 'Balasore',
        'Zirakhpur': 'Zirakpur',
        'Dayalpura sodhian': 'Zirakpur',
        'Mubarikpur camp': 'Zirakpur',
        'Bhubaneswar': 'Bhubaneshwar',
        'Burdge town': 'Midnapore',
        'Chhoto mathkatpur': 'Kharagpur',
        'Vijayawada': 'Vijaywada',
        'Una-himachal pradesh': 'Una',
        'Guwahati (t)' : 'Guwahati',
        'Panskura town' : 'Panskura',
        'Rampura phul' : 'Kapurthala',
        'Raipur pachimbar': 'Contai',
        'Edapalayam , chennai': 'Chennai',
        'Kappalur , madurai': 'Madurai',
        'T c balam, vanur taluk': 'Puducherry',
        'Annur taluk, coimbatore': 'Coimbatore',
        'Renigunta mandal, chittoor dt': 'Renigunta',
        'Thane - bhiwandi.' : 'Thane',
        'Ramji mandir chowk': 'Sambalpur',
        'Village daowra (hawrah)': 'Amta',
        'Virar east, dist - palghar': 'Virar',
        'Sankrail, nh-6, howrah': 'Sankrail',
        'Quthbullapur, ida jeedimelta': 'Secunderabad',
        'Kanjikuzhy_ kottayam': 'kottayam',
        'Bestan,surat,gujarat': 'Surat',
        'Viralimalai (pudukkotai dt.)': 'Viralimalai',
        'Paramathi velur-(po), namakkal-(dis': 'Namakkal',
        'Valliyoor (kkdt)': 'Valliyoor',
        'Ongole , prakasam': 'Ongole',
        'Pallipalayam , namakkal': 'Namakkal',
        'Sheoganj dist-sirohi': 'Sirohi',
        'Barani, bhagalpur': 'Bhagalpur',
        'Pipraich ((gorakhpur)': 'Gorakhpur',
        'Titlagarh dist-bolangir': 'Balangir',
        'Ramnagar, varanasi': 'Varanasi',
        'Fhullbari -siliguri': 'Siliguri',
        'Mavelikara , kerala': 'Mavelikara',
        'Langtlai-mizoram': 'Langtlai',
        'Kavundampalayam_ coimbatore': 'Coimbatore',
        '24 pgs (north)': 'Habra',
        '24 parganas(south)': 'Dakshin Barasat',
        'Chengannur_ kerala': 'Chengannur',
        '846004drabhanga': 'Darbhanga',
        "'jhajjar":'Jhajjar',
        'Allahaabad':'Allahabad',
        'Allahabad (fafamau)':'Allahabad',
        "'karimganj":'Karimganj'}
        
        df['Destination'] = df['Destination'].apply(lambda x: x.strip().capitalize())
        df['Destination'] = df['Destination'].apply(lambda x: DestMap[x] if x in DestMap.keys() else x)
        df['Destination'] = df['Destination'].apply(lambda x: x.replace(',', '_'))
        df = df[~df['Destination'].str.contains('^\d+')]  # drop 110006 etc 
        return df

    
    def imp_dist(df):
        dfj=df.copy()
        dfj=dfj[(dfj.Customer!='Sandbox')]
        dfj['distance']=dfj['distance'].apply(lambda x: np.nan if x<=0 else x)
        dfj['distance']=dfj['distance'].fillna(0)
        dfj['distance']=dfj.apply(lambda x:int(geodesic((x['Source Lat'],x['Source Long']),
                                                    (x['Dest Lat'],x['Dest Long'])).km) if x['distance']==0 else int(x['distance']) ,axis=1)

        return dfj 

    
    def lat_lon_prep(df):
    
        df.dropna(subset = ["depot_lat_long"], inplace=True)
 
        df.dropna(subset = ["consignee_lat_long"], inplace=True)

        df['Source Lat'] = df['depot_lat_long'].apply (lambda x: float(x.split(',')[0]) if float(x.split(',')[0])!=0 else np.nan)  
        df['Source Long'] = df['depot_lat_long'].apply (lambda x: float(x.split(',')[1])if float(x.split(',')[0])!=0 else np.nan)
        df['Dest Lat'] = df['consignee_lat_long'].apply (lambda x: float(x.split(',')[0])if float(x.split(',')[0])!=0 else np.nan)
        df['Dest Long'] = df['consignee_lat_long'].apply (lambda x: float(x.split(',')[1])if float(x.split(',')[0])!=0 else np.nan)
        
        df = df.drop(['depot_lat_long', 'consignee_lat_long'], axis='columns')
        df.dropna(subset=['Source Lat'])
        df.dropna(subset=['Source Long'])
        df.dropna(subset=['Dest Lat'])
        df.dropna(subset=['Dest Long'])
        df=df[(df['Source Lat'] > 8)&
                (df['Source Lat'] <32)&
                (df['Source Long']> 67)&
                (df['Source Long']<97)&
                (df['Dest Lat']>8)&
                (df['Dest Lat']<32)&
                (df['Dest Long']>67)&
                (df['Dest Long']<97)]
        df=df.drop_duplicates()
        df=df.reset_index().drop(['index'],1)
        return df

    def date_format(date):

        try:
            date_time=datetime.strptime(date.split('.')[0],'%Y-%m-%d %H:%M:%S' )
        except:
            pass
        try:
            date_time=datetime.strptime(date.split('+')[0],'%Y-%m-%d %H:%M:%S' )
        except:
            pass

        return date_time+timedelta(hours=5.5)    
    



    def update_hist_data(hist_data):
        data_from=str(max((hist_data['Created Date'].apply(lambda x:DataCorrectImputeUpdate.date_format(x))))+timedelta(1)).split(' ')[0]
        print('Fetching Data\nFrom:',data_from)
        now = datetime.now()
        today_date=(pd.to_datetime(now.strftime("%Y-%m-%d %H:%M:%S")))
        data_to=str(today_date-timedelta(1)).split(' ')[0]

        print('To:' ,data_to)
        c=-3
        try:
            dataset_url= 'http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(str(today_date-timedelta(3)).split(' ')[0],str(today_date-timedelta(2)).split(' ')[0])
            response = requests.post(dataset_url, data={},timeout=20)
        except:
            c=-2
            
        if c!=-2:
            try:
                dataset_url ='http://35.154.192.224:8090/lane_vt_report?from_date={}&to_date={}'.format(data_from,data_to)
                print(dataset_url)
                response = requests.post(dataset_url, data={})
            except:
                c=-1
                print('Database Connection Error: Unable To Update the Data')
            if c!=-1:
                try:
                    dfj=DataFrame(response.json())[working_columns]

                except:
                    c=0
                    print('Empty dataframe from Database')
                if c!=0:
                    try:
                        dfj=DataCorrectImputeUpdate.sourcemap(dfj)
                    except:
                        c=1
                        print('Correction Error: Source Map')
                    if c!=1:
                        try:
                            dfj=DataCorrectImputeUpdate.destmap(dfj)
                        except:
                            c=2
                            print('Correction Error: Destination Map')
                        if c!=2:
                            try:
                                dfj=DataCorrectImputeUpdate.Customer_name_correction(dfj) 
                            except:
                                c=3
                                print('Correction Error: Customer Name ')
                            if c!=3:
                                try:
                                    dfj=DataCorrectImputeUpdate.Transporter_name_correction(dfj)

                                except:
                                    c=4
                                    print('Correction Error: Transporter Name ')
                                if c!=4:
                                    try:
                                        dfj=DataCorrectImputeUpdate.lat_lon_prep(dfj)
                                        hist_data=hist_data.append(dfj)
                                        c=5
                                    except:
                                        print('Correction Error: Lat Long')

                                    
                                    
        if c==5:
            print('Data Sucessfully Updated')
        if c!=5:
            print('Unable to Update the Data')#raise ValueError('Unable to Update Data')
            

                            
                

        return hist_data
    


# In[7]:


## write monthly data in Data_Lane_VT_Monthly Folder
hist_data=DataCorrectImputeUpdate.data_pull()

## check for availability of history data, if available update it else work with old data
try:
    hist_data=pd.read_csv(os.path.join(processed_datafile_path,'history_data.csv'))
    hist_data=DataCorrectImputeUpdate.update_hist_data(hist_data)
    
    hist_data.to_csv(os.path.join(processed_datafile_path,'history_data.csv'),index=False)
except:
    dfj=DataCorrectImputeUpdate.sourcemap(hist_data)
    dfj=DataCorrectImputeUpdate.destmap(dfj)
    dfj=DataCorrectImputeUpdate.Customer_name_correction(dfj)   
    dfj=DataCorrectImputeUpdate.Transporter_name_correction(dfj)
    dfj=DataCorrectImputeUpdate.lat_lon_prep(dfj)
    dfj=DataCorrectImputeUpdate.imp_dist(dfj)
    dfj.to_csv(os.path.join(processed_datafile_path,'history_data.csv'),index=False)
    hist_data=pd.read_csv(   os.path.join(processed_datafile_path,'history_data.csv'))
    hist_data=DataCorrectImputeUpdate.update_hist_data(hist_data)
    hist_data.to_csv(os.path.join(processed_datafile_path,'history_data.csv'),index=False)


class Common_Features:
    
    def correctdtypes(df):
        for i in df.columns:
            if operator.contains(i,'month') or operator.contains(i,'weekday') or operator.contains(i,'hour')or operator.contains(i,'day') or operator.contains(i,'year'):
                df[i+'_continous']=df[i].astype('int32')
                df[i]=df[i].astype(str)
            if operator.contains(i,'Target') or operator.contains(i,'distance') or operator.contains(i,'Transit Time'):
                df[i]=df[i].astype('float32')
            if operator.contains(i,'Lat') or operator.contains(i,'Long'):
                df[i]=df[i].astype('float32')

        return df



    def hdays(df,column_name):
        holiday_date=[]

        holis = holidays.CountryHoliday('IND')
        for i,j in holis.items():
             holiday_date.append(i)
        list_holidays=Series([holis.get(i)  for i in df[column_name]])
        list_holidays=Series([i if i !=None else 'Normal' for i in list_holidays])
        df['holi']=list_holidays
        return df


    def row_to_col(df,column_name):
        list_df=[]
        df_dict={}
        for i in range(len(df.columns)):
            for i in df.iloc[:,i].values:
                list_df.append(i)
        df_dict[column_name]=list_df
        return pd.DataFrame(df_dict)

    def calender_days(df,input_column_name,output_column_name):
        df['{}month'.format(output_column_name)]=df[input_column_name].dt.month
        df['{}year'.format(output_column_name)]=df[input_column_name].dt.year
        df['{}hour'.format(output_column_name)]=df[input_column_name].dt.hour
        df['{}weekday'.format(output_column_name)]=df[input_column_name].dt.weekday
        df['{}day'.format(output_column_name)]=df[input_column_name].dt.day
        return df

    def lag_var(data, n_in=1, n_out=1, dropnan=True):
        n_vars = 1 if type(data) is list else data.shape[1]
        columns_df=data.columns
        df = DataFrame(data)
        cols, names = list(), list()

        for i in (range(n_in, 0, -1)):
            cols.append(df.shift(i))
            names += [(k+'_var(t-%d)' % (i)) for j,k in zip(range(n_vars),columns_df)]

        agg = concat(cols, axis=1)
        agg.columns = names

        if dropnan:
            agg.dropna(inplace=True)
        return agg


class DamageFreeShipment:
    
    def select_columns(hist_data):
        df_data=hist_data.drop(['Quantity',
                          #'Arrival Breached At',
                          #'Vehicle Type',
                          'contract_start_date',
                          'contract_end_date',
                          #'Indent ID',
                          #'Arrived At',
                          #'Dispatched At',
                          #'sla_delay_charges',
                          'contract_source',
                          #'Source Lat',
                          #'Source Long',
                          #'Dest Lat',
                          #'Dest Long',
                          #'Gross Weight',
                          #'Transit Time',
                          #'Delivery Date',
                          #'Customer',
                          'contract_destination',
                          #'contract_type',
                          #'Transporter',
                          #'Created Date',
                          #'Base Freight',
                          #'Actual Freight',
                          #'distance',
                          #'carton_damage_charges',
                          #'damage_charges',
                          'consignee_pincode',
                          'contract_id',
                          #'Destination',
                          #'Indent Type',
                          'contract_validity',
                          'Gross Volume',
                          #'shortage_charges',
                          #'Source'
                     ],1)
        return df_data
    
    def data_prep_ml(df_history):
        
        ## Placement Status
        df_history['Arrival Breached At']=df_history['Arrival Breached At'].fillna(0)
        df_history['Placement_Status']=df_history['Arrival Breached At'].apply(lambda x : 1 if x!=0 else x)
        df_history=df_history.drop(['Arrival Breached At'],1)
 
        df_history=df_history[~df_history.isna().any(axis=1)]
            
        df_history=df_history[(df_history.iloc[:, 0:] != 'NaT').all(axis=1)]
         
        df_history= df_history[df_history['Transporter'] != 'Dummy']
         
        df_history['Transporter']=df_history['Transporter'].apply(lambda x: x.strip().capitalize())
        df_history = df_history[~df_history['Destination'].str.contains('^\d+')]
        
        
        for j in ['Created Date','Arrived At','Dispatched At','Delivery Date'] :
            #print(j)
            df_history[j]=df_history[j].apply(lambda x:DataCorrectImputeUpdate.date_format(x))
            
        df_history['time_to_delv']=(df_history['Delivery Date']-df_history['Dispatched At']).dt.total_seconds()/3600
        
         
        df_contract=df_history[df_history['Indent Type']=='Contract']
        df_contract=df_history[df_history['Transit Time']!=0]
        df_contract['Transit Time']=df_contract['Transit Time']*24

        df_open=df_history[df_history['Indent Type']=='Open']
        
        #df_extra=df_history[~df_history['Indent Type'].isin(['Open', 'Contract'])]
        
        
        ## 'On time delivery Status'

        df_contract['OnTimeStatus']=df_contract[['Transit Time','time_to_delv']].apply(lambda x: 0 if x.time_to_delv>x['Transit Time'] else 1,axis=1)

        df_open['OnTimeStatus']=df_open['sla_delay_charges'].apply(lambda x: 0 if x>0 else 1)
        
        df_history=df_contract.append(df_open)
        
        ## Target Variable Damages
        df_history=df_history[df_history['Actual Freight']>0]
        df_history['Target']=((0.6*df_history['shortage_charges']) + (0.3*df_history['damage_charges']) + (0.1*df_history['carton_damage_charges']))/df_history['Actual Freight']
        
#         Create input features
 
        ## Indent Count total
        Total_indent_count=df_history.groupby(['Indent ID','Transporter']).count().reset_index().groupby(['Transporter']).count().reset_index()[['Transporter','Indent ID']]
        Total_indent_count.columns=['Transporter','Total_Indent_Count']

        ## Indent Count by route
        Indent_count_by_route=df_history.groupby(['Indent ID','Transporter','Source', 'Destination']).count().reset_index().groupby(['Transporter','Source', 'Destination']).count().reset_index()[['Transporter','Source', 'Destination','Indent ID']]
        Indent_count_by_route.columns=['Transporter','Source', 'Destination','Count_by_route']

        df_history=pd.merge(df_history,Total_indent_count, how='inner', on=None, left_on='Transporter', right_on='Transporter',
                 left_index=False, right_index=False, sort=False,
                 suffixes=('_x', '_y'), copy=True, indicator=False,
                 validate=None) 

        df_history=pd.merge(df_history,Indent_count_by_route, how='inner', on=None, left_on=['Transporter','Source', 'Destination'], right_on=['Transporter','Source', 'Destination'],
                 left_index=False, right_index=False, sort=False,
                 suffixes=('_x', '_y'), copy=True, indicator=False,
                 validate=None) 
        
        
        
        ## Special days
        df_history=Common_Features.hdays(df_history,'Created Date')
        df_history=Common_Features.hdays(df_history,'Dispatched At')
        df_history=Common_Features.hdays(df_history,'Delivery Date')


        ## creating calender days
        df_history=Common_Features.calender_days(df_history,'Created Date','creation')
        df_history=Common_Features.calender_days(df_history,'Created Date','Dispatched')
        df_history=Common_Features.calender_days(df_history,'Created Date','Delivery')


        ## Vehicle type Count
        dict_count={}
        vc=[]
        tn=[]
        for i in pd.unique(df_history['Transporter']):
            vc.append(len(pd.unique(df_history[df_history['Transporter']==i]['Vehicle Type'])))
            tn.append(i)
        dict_count['vehicle_count']=vc
        dict_count['transporter_name']=tn
        vehicle_count_df=DataFrame(dict_count)
        df_history=pd.merge(df_history,vehicle_count_df, how='inner', on=None, left_on='Transporter', right_on='transporter_name',
                 left_index=False, right_index=False, sort=False,
                 suffixes=('_x', '_y'), copy=True, indicator=False,
                 validate=None).drop(['transporter_name'],1)
        
        return df_history

    def data_prep_lag(df_data)   :
        # Lags Target
        df_arranged=df_data.sort_values(by='Created Date')


        Target_lags=10

        lags_df=Common_Features.lag_var(df_arranged[['Target']],n_in=Target_lags)
        lags_df=lags_df.reset_index().drop(['index'],1)

        df_arranged=df_arranged.iloc[Target_lags:,:]
        df_arranged=df_arranged.reset_index().drop(['index'],1)

        df_lagged=pd.concat([df_arranged,lags_df],1).dropna()

        #print(df_lagged['Transporter'])
        # # Crate Arrival Duration variable
        #df_lagged['arrival_duration']=((df_lagged['Arrived At']) - (df_lagged['Created Date'])).dt.total_seconds()/3600

        # Lags transportwise
        new_df=DataFrame()
        for i in pd.unique(df_lagged['Transporter']):
            #print(i)
            df_sample=(df_lagged[df_lagged['Transporter']==i]).sort_values(by='Created Date')

            ## Auto regressive effect
            lags=10

            lags_trans=Common_Features.lag_var(df_sample[['Source','Destination','Vehicle Type','Target','sla_delay_charges','creationmonth', 'creationyear','creationweekday', 'creationhour', 'creationday','Placement_Status','OnTimeStatus','Deliverymonth', 'Deliveryyear','Deliveryweekday', 'Deliveryhour', 'Deliveryday','Dispatchedmonth','Dispatchedyear','Dispatchedweekday', 'Dispatchedhour', 'Dispatchedday']],n_in=lags)  
            lags_trans=lags_trans.reset_index().drop(['index'],1)
            lags_trans.columns=lags_trans.columns+['_transporter_wise']
            lags_trans=lags_trans.reset_index().drop(['index'],1)

            df_sample=df_sample.iloc[lags:,:]
            df_sample=df_sample.reset_index().drop(['index'],1)

            df_laggedd=pd.concat([df_sample,lags_trans],1).dropna()
            new_df=new_df.append(df_laggedd)

        return new_df


## Machine Learning Variables Creation for Damage Free Shipment Prediction
df_data=DamageFreeShipment.select_columns(hist_data)
hist_data_ml_cost=DamageFreeShipment.data_prep_ml(df_data)
hist_data_ml_cost.to_csv(os.path.join(processed_datafile_path,'hist_data_ml_cost.csv'),index=False)

 

hist_data_ml_cost=Common_Features.correctdtypes(hist_data_ml_cost)

 
df=hist_data_ml_cost.drop(['Indent ID','Arrived At','Created Date','Delivery Date','Dispatched At'],1)
df_num=df[df.describe().columns]
df_obj=df.select_dtypes('object')

 
corr_matrix = df_num.drop(['Target'],1).corr().abs()

# Select upper triangle of correlation matrix
upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(np.bool))

# Find index of feature columns with correlation greater than 0.95
to_drop = [column for column in upper.columns if any(upper[column] > 0.95)]


 


# df_num=df_num.drop(to_drop,1)

 
DataFrame(df_num.drop(['Target'],1).columns,columns=['Num_Var']).to_csv(os.path.join(support_file_path,'Num_Var_DFS.csv'),index=False)
DataFrame(df_obj.columns,columns=['Obj_Var']).to_csv(os.path.join(support_file_path,'Obj_Var_DFS.csv'),index=False)

## Label encoding
d = defaultdict(LabelEncoder)

# Encoding the variable

df_obj_coded=df_obj.apply(lambda x: d[x.name].fit_transform(x))

##Joinin dataframe to get complete dataframe once again
df_lab=pd.concat([df_obj_coded,df_num],1)
df_lab.insert(0, 'Indent ID',hist_data_ml_cost['Indent ID']) 
df_lab.shape

pickle.dump(d, open(os.path.join(support_file_path,'label_coder_DFS.sav'), 'wb'))


df_lab=pd.concat([hist_data_ml_cost['Created Date'],df_lab],1)


df_lab=DamageFreeShipment.data_prep_lag(df_lab)
 

df_lab.to_csv(os.path.join(processed_datafile_path,'df_lab_DFS.csv'),index=False)



df_lab_ml=df_lab.drop(['creationmonth_continous',
                       'creationyear_continous',
                       'creationhour_continous',
                       'creationweekday_continous',
                       'creationday_continous',
                       'Dispatchedmonth_continous',
                       'Dispatchedyear_continous',
                       'Dispatchedhour_continous',  
                       'Dispatchedweekday_continous',  
                        'Dispatchedday_continous',  
                        'Deliverymonth_continous',  
                        'Deliveryyear_continous', 
                        'Deliveryhour_continous',  
                        'Deliveryweekday_continous',  
                        'Deliveryday_continous',
                       'creationmonth',
                       'creationyear',
                       'creationhour',
                       'creationweekday',
                       'creationday',
                        'Deliverymonth',  
                        'Deliveryyear',  
                        'Deliveryhour',  
                        'Deliveryweekday',  
                        'Deliveryday',  
                        'Dispatchedmonth', 
                        'Dispatchedyear',
                        'Dispatchedhour', 
                        'Dispatchedweekday', 
                        'Dispatchedday',
                        'time_to_delv',
                        'carton_damage_charges',
                        'shortage_charges',
                        'damage_charges',
                        'Actual Freight',
                       'sla_delay_charges',
                        'Base Freight'],1)

 
# ## Seperate Regression data


## Take regression data
df_reg=df_lab_ml[df_lab_ml.Target>0]
df_reg_ml=df_reg.drop(['Created Date'],1)

## Scale Regression data
predictor_matrix_reg=df_reg_ml.drop(['Target','Indent ID'],1).values
response_vector_reg=df_reg_ml['Target'].values.reshape(-1,1)
scaler_X_reg= MinMaxScaler(copy=True, feature_range=(0, 1))
scaler_Y_reg= MinMaxScaler(copy=True, feature_range=(0, 1))
predictor_scaled_matrix_reg=scaler_X_reg.fit_transform(predictor_matrix_reg)
response_scaled_vector_reg=scaler_Y_reg.fit_transform(response_vector_reg)

scaled_X_df_reg=pd.DataFrame(predictor_scaled_matrix_reg)
scaled_Y_df_reg=DataFrame(response_scaled_vector_reg,columns=['Target'])
columns_X=df_reg_ml.drop(['Target','Indent ID'],1).columns
scaled_X_df_reg.columns=columns_X
scaled_df_reg=pd.concat([df_reg_ml['Indent ID'].reset_index().drop(['index'],1)
                         ,scaled_X_df_reg,
                         scaled_Y_df_reg],1)



DataFrame(scaled_df_reg.drop(['Indent ID','Target',],1).columns.to_list(),columns=['variables']).to_csv(os.path.join(support_file_path,'variables_reg.csv'),index=False)
  
# save the scaler to disk
pickle.dump(scaler_X_reg, open(os.path.join(support_file_path,'scaler_X_reg.sav'), 'wb'))
pickle.dump(scaler_Y_reg, open(os.path.join(support_file_path,'scaler_Y_reg.sav'), 'wb'))


# ## Classification Problem: Random Forest Variable selection

## Target Variable Creation
df_class=df_lab_ml.copy()
df_class['Target'] = df_class['Target'].apply(lambda x: 0 if x==0 else 1)
df_class_ml=df_class.drop(['Created Date'],1)


# Drop 'Created Date','Indent ID' from df_lab to proceed

## Target Variable Creation
df_class=df_lab_ml.copy()
df_class['Target'] = df_class['Target'].apply(lambda x: 0 if x==0 else 1)
df_class_ml=df_class.drop(['Created Date'],1)

## Scale Classification data
predictor_matrix=df_class_ml.drop(['Target','Indent ID'],1).values
response_vector=df_class_ml['Target'].values.reshape(-1,1)
scaler_X= MinMaxScaler(copy=True, feature_range=(0, 1))
scaler_Y= MinMaxScaler(copy=True, feature_range=(0, 1))
predictor_scaled_matrix=scaler_X.fit_transform(predictor_matrix)
response_scaled_vector=scaler_Y.fit_transform(response_vector)

scaled_X_df=pd.DataFrame(predictor_scaled_matrix)
scaled_Y_df=DataFrame(response_scaled_vector,columns=['Target'])
columns_X=df_class_ml.drop(['Target','Indent ID'],1).columns
scaled_X_df.columns=columns_X
scaled_df=pd.concat([df_class_ml['Indent ID'].reset_index().drop(['index'],1)
                         ,scaled_X_df,
                         scaled_Y_df],1)

## write variables to local 
DataFrame(scaled_df.drop(['Indent ID','Target',],1).columns.to_list(),columns=['variables']).to_csv(os.path.join(support_file_path,'variables_classif.csv'),index=False)

## Balance Classes
from sklearn.utils import resample

# Separate majority and minority classes
df_majority = scaled_df[scaled_df.Target==0]
df_minority = scaled_df[scaled_df.Target==1]
 
# Upsample minority class
df_minority_upsampled = resample(df_minority, 
                                 replace=True,     # sample with replacement
                                 n_samples=int(0.6*len(df_majority)),    # to match majority class
                                 random_state=123) # reproducible results
 
# Combine majority class with upsampled minority class
df_upsampled = pd.concat([df_majority, df_minority_upsampled])
 
# Display new class counts
scaled_df=df_upsampled
print(scaled_df.Target.value_counts())


## Train Test Split
import random 
test_orders=random.sample(pd.unique(scaled_df['Indent ID']).tolist(),int(0.2*len(pd.unique(scaled_df['Indent ID']))))

Test_df=scaled_df.loc[scaled_df['Indent ID'].isin(test_orders)]
Train_df=scaled_df[~scaled_df['Indent ID'].isin(test_orders)]
Test_df=Test_df.drop(['Indent ID'],1)
Train_df=Train_df.drop(['Indent ID'],1)

spm=Train_df.drop(['Target'],1).values
stm=Train_df['Target'].values
print(set(stm))

# ## Select Best variable
import pandas as pd
import numpy as np
from pandas import read_csv,DataFrame,concat,Series
from sklearn.ensemble import RandomForestClassifier 
import pickle
import holidays
import warnings
import requests
from sklearn.model_selection import GridSearchCV
from sklearn.linear_model import LogisticRegression,LogisticRegressionCV
from tensorflow.keras import Sequential
from tensorflow.keras.layers import Dense,Dropout
from tensorflow.keras.callbacks import ModelCheckpoint
from tensorflow.keras.models import load_model
from sklearn.metrics import classification_report
warnings.filterwarnings("ignore")



class ml_model:
    def feat_select(x,y,Train_df,n_top):
        feat_select_model = RandomForestClassifier(n_estimators=200, 
                               bootstrap = True,
                               max_features = 'sqrt',verbose=1)
        feat_select_model.fit(x,y)
        feat_df=pd.concat([Series(feat_select_model.feature_importances_),Series(Train_df.drop(['Target'],1).columns)],1)
        feat_df.columns=['score','feature']
        feat_df=feat_df.sort_values(by='score',ascending=False)
                
        return feat_df.feature[0:n_top]
    
#     def feat_select_grid_search(x,y,Train_df,n_top):  

#         param_grid = {'n_estimators': [400,500],
#                       'max_features': ['auto', 'sqrt', 'log2'],
#                       'max_depth' : [4,5,6,7,8],
#                       'criterion' :['gini', 'entropy']}
#         RF= RandomForestClassifier(random_state=42)
#         CV_rfc = GridSearchCV(estimator=RF, param_grid=param_grid, cv= 5)
#         CV_rfc.fit(x, y)
#         best_hyp=CV_rfc.best_params_
#         feat_select_model=RandomForestClassifier(random_state=42, max_features=best_hyp['max_features'], n_estimators= best_hyp['n_estimators'], max_depth=best_hyp['max_depth'], criterion=best_hyp['criterion'])
#         feat_select_model.fit(x,y)
#         feat_df=pd.concat([Series(feat_select_model.feature_importances_),Series(Train_df.drop(['Target'],1).columns)],1)
#         feat_df.columns=['score','feature']
#         feat_df=feat_df.sort_values(by='score',ascending=False)

        
#         return feat_df.feature[0:n_top]
    
#     def DNN(x,y,filepath):

#         model = Sequential()
#         model.add(Dropout(0.5, input_shape=(x.shape[1],)))
#         model.add(Dense(4, activation='relu'))
#         model.add(Dense(4, activation='relu'))
# #         model.add(Dense(2, activation='relu'))
#         model.add(Dense(y.shape[1], activation='sigmoid'))
#         model.compile(loss='binary_crossentropy', optimizer='adam')

#         # Monitor validation loss
#         checkpoint = ModelCheckpoint(filepath, monitor='val_loss', verbose=1, save_best_only=True, mode='min')

#         # can use early stop as well instead of checkpoint
#         callbacks_list = [checkpoint]

#         # fit model
#         model.fit(x, y,
#                   validation_split=0.33,
#                   epochs=200,
#                   batch_size=2**6,
#                   verbose=1,callbacks=callbacks_list)
#         # # # load the saved model
#         saved_model = load_model(filepath)


        return saved_model
    
    def Logit(x,y,filename):
        
        logreg = LogisticRegression(C=1)
        model=logreg.fit(x,y)

        # save the model to disk
        pickle.dump(logreg, open(os.path.join(model_file_path,filename), 'wb'))
        
        # load the model from disk
        model= pickle.load(open(os.path.join(model_file_path,filename), 'rb'))
        
        return model
    
#     def Logit_grid(x,y,filename):
#         grid={"C":np.logspace(-3,3,7),
#               "penalty":["l1","l2"]}

#         logreg=LogisticRegression()
        
#         logreg_cv=GridSearchCV(logreg,grid,cv=10)
#         logreg_cv.fit(x_train,y_train)
#         best_hyp=logreg_cv.best_params_
#         logreg = LogisticRegression(C=best_hyp['C'],penalty=best_hyp['penalty'])
#         model=logreg.fit(x,y)

#         # save the model to disk
#         pickle.dump(logreg, open(filename, 'wb'))
        
#         # load the model from disk
#         model= pickle.load(open(filename, 'rb'))
    
    

    def report(model,y,y_hat):
        
        return classification_report(y, y_hat)  


best_features=ml_model.feat_select(spm,stm,Train_df,n_top=50)
best_var_df=DataFrame(best_features).reset_index().drop(['index'],1)
best_var_df.columns=['Lab_variables']
best_var_df.to_csv(os.path.join(support_file_path,'variables_best_DFS.csv'),index=False)

 
# save the scaler to disk

pickle.dump(scaler_X, open(os.path.join(support_file_path,'scaler_X_class.sav'), 'wb'))
pickle.dump(scaler_Y, open(os.path.join(support_file_path,'scaler_Y_class.sav'), 'wb'))


best_features_lst=best_features.tolist() 
best_features_lst.append('Target')
df_best_train=Train_df[best_features_lst]
df_best_test=Test_df[best_features_lst]
predictor_scaled_matrix=df_best_train.drop(['Target'],1).values
response_scaled_vector=df_best_train['Target'].values.reshape(-1,1)
model_LR=ml_model.Logit(predictor_scaled_matrix,response_scaled_vector,filename ='DFS_prob_LR_model.sav')

fd=pd.DataFrame(df_best_train.drop(['Target'],1).columns,columns=['Features'])


fd['coeff']=(model_LR.coef_)[0]


fd.to_csv(os.path.join(model_out_path,'DFS_coeff_lab.csv'),index=False) 


# ## Performace on Test Data

predictor_matrix_best_test=df_best_test.drop(['Target'],1).values
response_matrix_test=df_best_test['Target'].values.reshape(-1,1)

model_LR=pickle.load(open(os.path.join(model_file_path,'DFS_prob_LR_model.sav'), 'rb'))
 
## Logistic Reg
y_hat_logit = scaler_Y.inverse_transform(model_LR.predict(predictor_matrix_best_test).reshape(-1,1))
prob_logit=model_LR.predict_proba(predictor_matrix_best_test)[:,1]
 

report_LR=ml_model.report(model_LR,scaler_Y.inverse_transform(response_matrix_test),y_hat_logit)
# print(report_LR)


# In[48]:


import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import roc_auc_score
from sklearn.metrics import roc_curve


# In[ ]:



class plots:
    
    def dens_box_plot(df1):
        for i, col in enumerate(df1):
            plt.figure(i)
            sns.distplot(df1[col], kde=True, color="b")
            plt.savefig(os.path.join(model_out_path,'Desity_DFS.png'))
#             plt.show()
            sns.boxplot(df1[col], color="b")
            #plt.savefig('BoxPlot_{}'.format(col))
            print (df1.describe()[col])
    def Heat_Map(df1) :
        correlations = df1.corr()
        plt.figure(figsize=(10,10))
        sns.heatmap(round(correlations,2),vmin=-1,cmap='coolwarm',annot=True,)
    
    def ROC_curve(y,y_hat,p,label):

        logit_roc_auc = roc_auc_score(y,y_hat)
        fpr, tpr, thresholds = roc_curve(y,p)
        plt.figure()
        plt.plot(fpr, tpr, label=label+' '+'(area = %0.2f)' % logit_roc_auc)
        plt.plot([0, 1], [0, 1],'r--')
        plt.xlim([0.0, 1.0])
        plt.ylim([0.0, 1.05])
        plt.xlabel('False Positive Rate')
        plt.ylabel('True Positive Rate')
        plt.title('Receiver operating characteristic')
        plt.legend(loc="lower right")
        plt.savefig(os.path.join(model_out_path,'ROC_DFS.png'))
#         plt.show()



# plots.ROC_curve(response_matrix_test,y_hat_dnn,prob_dnn,label='Deep Neural Net')
plots.ROC_curve(response_matrix_test,y_hat_logit,prob_logit,label='Logistic Regression')


# In[49]:


plots.dens_box_plot(DataFrame(1-prob_logit,columns=['Damage_Free_Shipment_Distribution_LR']))


# ## Build Regression Model

# In[50]:


## Remove Outlier

def remove_outlier(df_in, col_name):
   q1 = df_in[col_name].quantile(0.25)
   q3 = df_in[col_name].quantile(0.75)
   iqr = q3-q1 #Interquartile range
   fence_low  = q1-1.5*iqr
   fence_high = q3+1.5*iqr
   print ('\nLower Limit :',fence_low, 'Upper Limit :', fence_high)
   df_out_lower = df_in.loc[ df_in[col_name] < fence_low]
   df_out_upper = df_in.loc[ df_in[col_name] > fence_high]
   

   if (df_out_lower.shape[0] > 0 and df_out_upper.shape[0] >0) :
   
       return concat([df_out_lower,df_out_upper],0)

   if (df_out_lower.shape[0] >0 and df_out_upper.shape[0] == 0) :
       return df_out_lower

   if (df_out_lower.shape[0]==0 and df_out_upper.shape[0] > 0) :
       return df_out_upper

   else:
       return pd.DataFrame(columns=['Nothing'])
   
   
Outlier_df=DataFrame(columns=scaled_df_reg.columns)
a=remove_outlier(scaled_df_reg,'Target')
Outlier_df=concat([Outlier_df,a],0)

N0_outlier_rows=len(Outlier_df)
print ('Outlier Rows :',N0_outlier_rows,'\nOutlier Data Percentage :', (N0_outlier_rows/len(scaled_df_reg))*100)
print ('Final number of rows available after removing Outliers: ',len(scaled_df_reg)-N0_outlier_rows)

scaled_df_reg=pd.merge(scaled_df_reg,Outlier_df,indicator=True,how='outer').query('_merge=="left_only"').drop('_merge', axis=1)


## Split Train Test data

test_orders=random.sample(pd.unique(scaled_df_reg['Indent ID']).tolist(),int(0.2*len(pd.unique(scaled_df_reg['Indent ID']))))

Test_df_reg=scaled_df_reg.loc[scaled_df_reg['Indent ID'].isin(test_orders)]
Train_df_reg=scaled_df_reg[~scaled_df_reg['Indent ID'].isin(test_orders)]

Test_df_reg=Test_df_reg.drop(['Indent ID'],1)
Test_df_reg_best=Test_df_reg#[best_features_lst]

Train_df_reg=Train_df_reg.drop(['Indent ID'],1)
Train_df_reg_best=Train_df_reg#[best_features_lst]
DataFrame(Train_df_reg_best.drop(['Target'],1).columns,columns=['DFSRegression_Var']).to_csv(os.path.join(support_file_path,'DFSRegression_Var.csv'),index=False)




## Fit Regression Model
from sklearn.linear_model import Ridge
import random 
from sklearn.metrics import mean_squared_error
def ridge_reg(x,y,filename):
   model = Ridge(alpha=5)
   model.fit(x, y)
   
   # save the model to disk
   pickle.dump(model, open(os.path.join(model_file_path,filename), 'wb'))

   # load the model from disk
   model= pickle.load(open(os.path.join(model_file_path,filename), 'rb'))

   return model



predictor_matrix_train_reg=Train_df_reg_best.drop(['Target'],1).values
response_matrix_train_reg=Train_df_reg_best['Target'].values.reshape(-1,1)

predictor_matrix_test_reg=Test_df_reg_best.drop(['Target'],1).values
response_matrix_test_reg=Test_df_reg_best['Target'].values.reshape(-1,1)

model_ridge=ridge_reg(predictor_matrix_train_reg,response_matrix_train_reg,filename ='DFS_ridge_model.sav')


## test data validation
y_hat_ridge=scaler_Y_reg.inverse_transform(model_ridge.predict(predictor_matrix_test_reg).reshape(-1,1))
y_hat_ridge[y_hat_ridge<0]=0

## Metric

mse_ridge=np.sqrt(mean_squared_error(scaler_Y_reg.inverse_transform(response_matrix_test_reg), y_hat_ridge))
print(mse_ridge)
