# -*- coding: utf-8 -*-
"""


@author: Jason Beattie



need to get a price based of run time (will need 2nd compressor data)

free air cooling time  / year or time peroid % of time in free cooling

actual time in free cooling

price for whole year


"""


import pandas as pd
from datetime import datetime

#PR_num = '3_1' # Plant Room Number
temp_threshold = 24.0   # delete rows where ambient is above this valve



def csv_run(plant_room):
    
    '''
    csv_run cleans up the data and merges it into a single CSV

    '''
    print('CSV Cleanup')    
    
    # Import

                  #ambient temp df
    amb_csv = './amb/' + plant_room + '_amb.csv'
    comp_csv = './comp/' + plant_room + '_Comp.csv'
    kWhrs_csv = './kWhrs/' + plant_room + '_kWhrs.csv'
    
    df_amb = pd.read_csv(amb_csv, index_col=0, skiprows=3)  
    df_Comp = pd.read_csv(comp_csv, index_col=0, skiprows=3)                    # Compressor enable df
    df_kWhrs = pd.read_csv(kWhrs_csv, index_col=0, skiprows=3)                  # kwHrs
    
    # Clean up CSVs
    
    del df_amb['Trend Flags']                                                   # Delet Colum
    del df_amb['Status']                                                        # Delet Colum
    df_amb.columns = ['Temp']                                                   # Rename colum
    df_amb['Temp'] = df_amb['Temp'].str.replace(' °C', '')                      #remove unit
    df_amb['Temp'] = df_amb['Temp'].astype('float')                                      #convert to float
     
    
    del df_Comp['Trend Flags']                                                  # Delet Colum
    del df_Comp['Status']                                                       # Delet Colum
    df_Comp.columns = ['Enable']                                                # Rename colum
        
    
    del df_kWhrs['Trend Flags']                                                 # Delet Colum
    del df_kWhrs['Status']                                                      # Delet Colum
    df_kWhrs.columns = ['kWhrs']                                                # Rename colum
    df_kWhrs['kWhrs'] = df_kWhrs['kWhrs'].str.replace('kW-hr', '')              # remove unit
    df_kWhrs['kWhrs'] = df_kWhrs['kWhrs'].str.replace(',', '')                  # remove common
    df_kWhrs['kWhrs'] = df_kWhrs['kWhrs'].astype('float')                       # convert to float
    
    
    # Join dataframes togther
    
    merged_df = pd.merge(df_amb, df_Comp, on='Timestamp', how='outer')
    merged_df = pd.merge(merged_df, df_kWhrs, on='Timestamp', how='outer')
    
    merged_df.index = pd.to_datetime(merged_df.index)                           # Convert index to datetime note this is what casues the error
    
    merged_df = merged_df.sort_values(by=['Timestamp'])                         # Sort all data by datetime
    
    # Fill in blank data caused by merge
    
    merged_df = merged_df.fillna(method="pad")                                  # Foward Fill
    merged_df = merged_df.fillna(method="bfill")                                # Backfill remaining
    
    csv_new = './merged/' + plant_room + '_merged_data.csv'
            
    merged_df.to_csv(csv_new)                                                   # Output merged/cleaned CSV                



def filter_data(plant_room):

    '''
    
    'filter_data' uses the generated CSV from 'csv_run' to filter data that is
    below the 'temp_threshold' 
    
    
    '''
    print('Data Filter on:', plant_room)
    
    
    csv_merged = './merged/' + plant_room + '_merged_data.csv'                                # import CSV  
    
    merged_df = pd.read_csv(csv_merged, index_col=0)                            # create DF
    
    
    # Need to delete rows are above temp threshold
    
    index_names = merged_df[merged_df['Temp'] > temp_threshold ].index          # Find all rows Temp < Threshold Value
    merged_df.drop(index_names, inplace = True)                                 # Delete all rows below threshold value
    
    
    # Remove entires where compressor is Off
    
    '''
    This section was removed - as the compressor off state was needed to 
    calculate the kWhrs at every 'RUN' / Comp = On event
    
    '''
    
    #index_names = merged_df[merged_df['Enable'] == 'Off' ].index               # Find all rows where
    #merged_df.drop(index_names, inplace = True)                                # Delete all values where compressor is off

   
    csv_new = './filtered_data/' +plant_room + '_filtered_data.csv'
    merged_df.to_csv(csv_new)



def calcs(plant_room):
    
    '''
    
    'Calcs' 
    
    '''
    
    print('Running calcs on:', plant_room)                                      
    csv_filter = './filtered_data/' + plant_room + '_filtered_data.csv'                     # Read CSV
        
    merged_df = pd.read_csv(csv_filter, index_col=0)                            # Create DF
   
    
    csv_merged = './merged/' + plant_room + '_merged_data.csv'                                # import CSV  
    
    unfiltered_df = pd.read_csv(csv_merged, index_col=0)  
    
    
    
    fmt = '%Y-%m-%d %H:%M:%S'
    
    row_int = 0
    row_int_1 = 0                                                                 # DF Row intiger counter
    latch = 1                                                                   # Compressor ON latch - this gets set to 0, after the 1st COMP==ON state, and resets when COMP == OFF
    latch_tot = 0                                                               # Totalizer latch - stops the kWhrs totalizer from counting when COMP ==OFF
    econ_time = 0
    
    comp_starts = 0                                                                                                                     
    kWhrs_totalizer = 0                                                     
    kWhr_start = 0
    kWhr_end = 0
    comp_time = 0
    
    
    #Print head and tail of DF
    #head = pd.DataFrame(unfiltered_df.head(1))
    #print(head[0,1])
    #print(unfiltered_df.tail(1))
    # https://stackoverflow.com/questions/36542169/extract-first-and-last-row-of-a-dataframe-in-pandas
    
    # Calculate Ideal Time in Free Cooling - only need to do 1 PR
    
    for index, row in unfiltered_df.iterrows():
        
        temp = unfiltered_df.iat[row_int,0]
        #print(temp)
        
        if temp < temp_threshold:
            #print(temp)
    
            if latch == 1:                                                      # Only record the kWhrs of the 1st instance of a Compressor run event
                
                timestamp_start = unfiltered_df.index[row_int]
                #print(timestamp_start)
                #print(merged_df.iat[row_int,0])
                timestamp_start = datetime.strptime(timestamp_start, fmt)
                
                                         # Load kWhr reading of compressor run event
                
                latch = 0                                                       # set latch to 0 
                                                            # Count up compressor starts
                latch_tot = 1                                                   # Set totalizer latch so end kWhrs can be recorded when compressor turns off
        
        
        else:                                                                   # Compressors == OFF
            latch = 1                                                           # Reset latch so can get next compressor on kWhrs
            
            
            if latch_tot == 1:                                                  # only records kWhrs of the end of a compressor Run event
                         
                
                            # Load kWhrs readings
                timestamp_end = unfiltered_df.index[row_int - 1]
                
                #print(timestamp_end)
                #print(merged_df.iat[row_int - 1 ,0]) ######## NOTE  added the -1 on the row_int to give accurate row
                
                timestamp_end = datetime.strptime(timestamp_end, fmt)
                td =  timestamp_end - timestamp_start
                
                td_sec = int(round(td.total_seconds()))
                econ_time = td_sec + econ_time
                #print(td_sec/60)
                #print('delta t',td_mins)
                # Add to total

                latch_tot = 0                                                   # Reset totalizer latch
            
       
        
        
        row_int += 1     
    
    
    
    # Reset latches
    latch = 1                                                                   # Compressor ON latch - this gets set to 0, after the 1st COMP==ON state, and resets when COMP == OFF
    latch_tot = 0       
    row_int = 0
    for index, row in merged_df.iterrows():                                     # Iterate through DF
    
        comp_status = merged_df.iat[row_int,1]                                  # Returns the Comp Status ON/OFF of the compressor in the row 
        
        
        if comp_status == 'On':
            if latch == 1:                                                      # Only record the kWhrs of the 1st instance of a Compressor run event
                
                timestamp_start = merged_df.index[row_int]
                #print(timestamp_start)
                #print(merged_df.iat[row_int,0])
                timestamp_start = datetime.strptime(timestamp_start, fmt)
                
                kWhr_start = merged_df.iat[row_int,2]                           # Load kWhr reading of compressor run event
                
                latch = 0                                                       # set latch to 0 
                comp_starts +=1                                                 # Count up compressor starts
                latch_tot = 1                                                   # Set totalizer latch so end kWhrs can be recorded when compressor turns off
        
        
        else:                                                                   # Compressors == OFF
            latch = 1                                                           # Reset latch so can get next compressor on kWhrs
            
            
            if latch_tot == 1:                                                  # only records kWhrs of the end of a compressor Run event
                         
                kWhr_end = merged_df.iat[row_int - 1,2] 
                            # Load kWhrs readings
                timestamp_end = merged_df.index[row_int - 1]
                
                #print(timestamp_end)
                #print(merged_df.iat[row_int - 1 ,0]) ######## NOTE  added the -1 on the row_int to give accurate row
                
                timestamp_end = datetime.strptime(timestamp_end, fmt)
                td =  timestamp_end - timestamp_start
                
                td_sec = int(round(td.total_seconds()))
                #print('delta t',td_mins)
                comp_time = comp_time + td_sec
                
                kWhr_diff = kWhr_end - kWhr_start                               # Differance in kWhrs over run event

                kWhrs_totalizer = kWhrs_totalizer + kWhr_diff                   # Add to total

                latch_tot = 0                                                   # Reset totalizer latch
            
       
        
        
        row_int += 1                                                            # Count up row integar
        
    
     
    
    kwhr_t = kWhrs_totalizer
       
        
    # Caculate kWhr wasteage -----------------------------------------------------
    power_price = 0.14 # c/kWhr
    
    comp_time = comp_time / 60
    price_waste = kwhr_t * power_price
    
    print( "price:",round(price_waste))
    
    print('kWhrs:', round(kwhr_t))
    
    print( 'c/kWhr:', power_price)
    
    print('Comp Starts',comp_starts)
    
    print('Comp run time(hr) ',round(comp_time/60))
    
    print('Econ Time:', (econ_time/60)/60)
    return price_waste



#Uncomment Bellow if data is to be run

#PR_list = ['2_1','2_2']
PR_list = ['2_1','2_2','2_3','2_4', '3_1', '3_2', '3_3', '3_4','4_1','4_3']
#PR_list = ['3_1', '3_2', '3_3', '3_4','4_1','4_3']

length = len(PR_list)
price_tot = 0
for i in range(length):
     
    
    PR_num = PR_list[i]
    print(PR_num)
    #csv_run(PR_num) # Can uncomment once data is run once
    #(PR_num)
    filter_data(PR_num)
    price_tot = price_tot + calcs(PR_num)
    
    
print('dollerydoos:',round(price_tot))

# Jasons Note: This software was written during my personal time

#csv_run(PR_num) 
#filter(PR_num)
#print(PR_num)
#filter_data(PR_num)
#calcs(PR_num)
