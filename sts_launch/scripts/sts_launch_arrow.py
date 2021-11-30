#!/usr/bin/env python3
import rospy
import message_filters
from rospy.rostime import get_time
from sensor_msgs.msg import Image
from sts_balanceboard_test.msg import bboard_data
from geometry_msgs.msg import Pose, PoseStamped
from geometry_msgs.msg import PoseArray
import pandas as pd
from seating_mat_driver.msg import seating_data
from xsens_streamer.msg import xsens_long
import numpy as np
import copy
from datetime import datetime
import pyarrow as pa
import pyarrow.csv as csv


rospy.init_node('Launch')


def seat_callback(msg):
    global seat_data
    global seat_frame
    global seat_frame_arrow

    seat_head = pd.DataFrame(data=[msg.header.stamp])
    seat_data = msg.data
    seat_data = pd.DataFrame(seat_data)
    seat_data = seat_data.transpose()
    seat_data = pd.concat([seat_head, seat_data], axis=1)
    seat_frame = copy.deepcopy(seat_data)

    seat_frame_arrow = seat_frame.copy()
    seat_frame_arrow = pa.Table.from_pandas(seat_frame)
   
    

def balance_callback(msg):  
    global balance_head
    global balance_data
    global balance_frame
    global balance_check
    global balance_frame_arrow

    balance_check = 1

    balance_head = copy.deepcopy(msg.header.stamp)
    balance_data = [balance_head, msg.front_left, msg.front_right, msg.rear_left, msg.rear_right]
    balance_data = pd.DataFrame(balance_data)
    balance_data = balance_data.transpose()
    balance_frame = copy.deepcopy(balance_data)

    balance_frame_arrow = balance_frame.copy()
    balance_frame_arrow = pa.Table.from_pandas(balance_frame)
    


def xsens_callback(msg): ##Assuming that this is the fastest sensor
    global xsens_data 
    global xsens_head
    global count_test
    global xsens_frame
    global xsens_frame_arrow


    xsens_head = pd.DataFrame(data=[msg.header.stamp])
    xsens_data = msg.data
    xsens_data = pd.DataFrame(xsens_data)
    xsens_data = xsens_data.transpose()
    xsens_data = pd.concat([xsens_head, xsens_data], axis=1)
    xsens_frame = copy.deepcopy(xsens_data)


    # xsens_frame.iloc[0, 0] = xsens_frame.iloc[0, 0].values.astype(np.int64) // 10**9   #pyarrow tests
    xsens_frame_arrow = xsens_frame.copy()
    xsens_frame_arrow = pa.Table.from_pandas(xsens_frame)








def read_sensors():
    global count_test
    global balance_check

    
    
    rate = rospy.Rate(100)

    balance_check = 0


    #create headers for each csv file
    header_head = pd.DataFrame(data=['Header'], columns=['Header'])
    seat_csv_head = np.arange(256)
    seat_csv_head = pd.DataFrame(seat_csv_head)
    seat_csv_head = seat_csv_head.transpose()
    seat_csv_head = pd.concat([header_head, seat_csv_head], axis=1)
    seat_csv_head.to_csv(fr'~/Documents/{part_input}_{trial_input}_Seat.csv', mode='w', header=False, index=False) 

    balance_csv_head = ['Header', 'Front_left', 'Front_Right', 'Rear_Left', 'Rear_Right']
    balance_csv_head = pd.DataFrame(balance_csv_head)
    balance_csv_head = balance_csv_head.transpose()
    balance_csv_head.to_csv(fr'~/Documents/{part_input}_{trial_input}_Balance.csv', mode='w', header=False, index=False)

    xsens_csv_head = ['field.header.stamp', 'field.poses0.position.x', 'field.poses0.position.y', 'field.poses0.position.z',
 'field.poses0.orientation.x', 'field.poses0.orientation.y', 'field.poses0.orientation.z', 'field.poses0.orientation.w',
 'field.poses1.position.x', 'field.poses1.position.y', 'field.poses1.position.z',
 'field.poses1.orientation.x', 'field.poses1.orientation.y', 'field.poses1.orientation.z', 'field.poses1.orientation.w',
 'field.poses2.position.x', 'field.poses2.position.y', 'field.poses2.position.z',
 'field.poses2.orientation.x', 'field.poses2.orientation.y', 'field.poses2.orientation.z', 'field.poses2.orientation.w',
 'field.poses3.position.x', 'field.poses3.position.y', 'field.poses3.position.z',
 'field.poses3.orientation.x', 'field.poses3.orientation.y', 'field.poses3.orientation.z', 'field.poses3.orientation.w',
 'field.poses4.position.x', 'field.poses4.position.y', 'field.poses4.position.z',
 'field.poses4.orientation.x', 'field.poses4.orientation.y', 'field.poses4.orientation.z', 'field.poses4.orientation.w',
 'field.poses5.position.x', 'field.poses5.position.y', 'field.poses5.position.z',
 'field.poses5.orientation.x', 'field.poses5.orientation.y', 'field.poses5.orientation.z', 'field.poses5.orientation.w',
 'field.poses6.position.x', 'field.poses6.position.y', 'field.poses6.position.z',
 'field.poses6.orientation.x', 'field.poses6.orientation.y', 'field.poses6.orientation.z', 'field.poses6.orientation.w',
 'field.poses7.position.x', 'field.poses7.position.y', 'field.poses7.position.z',
 'field.poses7.orientation.x', 'field.poses7.orientation.y', 'field.poses7.orientation.z', 'field.poses7.orientation.w',
 'field.poses8.position.x', 'field.poses8.position.y', 'field.poses8.position.z',
 'field.poses8.orientation.x', 'field.poses8.orientation.y', 'field.poses8.orientation.z', 'field.poses8.orientation.w',
 'field.poses9.position.x', 'field.poses9.position.y', 'field.poses9.position.z',
 'field.poses9.orientation.x', 'field.poses9.orientation.y', 'field.poses9.orientation.z', 'field.poses9.orientation.w',
 'field.poses10.position.x', 'field.poses10.position.y', 'field.poses10.position.z',
 'field.poses10.orientation.x', 'field.poses10.orientation.y', 'field.poses10.orientation.z', 'field.poses10.orientation.w',
 'field.poses11.position.x', 'field.poses11.position.y', 'field.poses11.position.z',
 'field.poses11.orientation.x', 'field.poses11.orientation.y', 'field.poses11.orientation.z', 'field.poses11.orientation.w',
 'field.poses12.position.x', 'field.poses12.position.y', 'field.poses12.position.z',
 'field.poses12.orientation.x', 'field.poses12.orientation.y', 'field.poses12.orientation.z', 'field.poses12.orientation.w',
 'field.poses13.position.x', 'field.poses13.position.y', 'field.poses13.position.z',
 'field.poses13.orientation.x', 'field.poses13.orientation.y', 'field.poses13.orientation.z', 'field.poses13.orientation.w',
 'field.poses14.position.x', 'field.poses14.position.y', 'field.poses14.position.z',
 'field.poses14.orientation.x', 'field.poses14.orientation.y', 'field.poses14.orientation.z', 'field.poses14.orientation.w',
 'field.poses15.position.x', 'field.poses15.position.y', 'field.poses15.position.z',
 'field.poses15.orientation.x', 'field.poses15.orientation.y', 'field.poses15.orientation.z', 'field.poses15.orientation.w',
 'field.poses16.position.x', 'field.poses16.position.y', 'field.poses16.position.z',
 'field.poses16.orientation.x', 'field.poses16.orientation.y', 'field.poses16.orientation.z', 'field.poses16.orientation.w',
 'field.poses17.position.x', 'field.poses17.position.y', 'field.poses17.position.z',
 'field.poses17.orientation.x', 'field.poses17.orientation.y', 'field.poses17.orientation.z', 'field.poses17.orientation.w',
 'field.poses18.position.x', 'field.poses18.position.y', 'field.poses18.position.z',
 'field.poses18.orientation.x', 'field.poses18.orientation.y', 'field.poses18.orientation.z', 'field.poses18.orientation.w',
 'field.poses19.position.x', 'field.poses19.position.y', 'field.poses19.position.z',
 'field.poses19.orientation.x', 'field.poses19.orientation.y', 'field.poses19.orientation.z', 'field.poses19.orientation.w',
 'field.poses20.position.x', 'field.poses20.position.y', 'field.poses20.position.z',
 'field.poses20.orientation.x', 'field.poses20.orientation.y', 'field.poses20.orientation.z', 'field.poses20.orientation.w',
 'field.poses21.position.x', 'field.poses21.position.y', 'field.poses21.position.z',
 'field.poses21.orientation.x', 'field.poses21.orientation.y', 'field.poses21.orientation.z', 'field.poses21.orientation.w',
 'field.poses22.position.x', 'field.poses22.position.y', 'field.poses22.position.z',
 'field.poses22.orientation.x', 'field.poses22.orientation.y', 'field.poses22.orientation.z', 'field.poses22.orientation.w']
    xsens_csv_head = pd.DataFrame(xsens_csv_head)
    xsens_csv_head = xsens_csv_head.transpose()
    xsens_csv_head.to_csv(fr'~/Documents/{part_input}_{trial_input}_Xsens.csv', mode='w', header=False, index=False)
    



    rospy.Subscriber('seating_raw', seating_data, callback=seat_callback, queue_size=1)  
    rospy.Subscriber('balance_board', bboard_data, callback=balance_callback, queue_size=1)  
    rospy.Subscriber('xsens_array', xsens_long, callback=xsens_callback, queue_size=1)
    #rospy.Subscriber('xsens_data', PoseArray, callback=xsens_callback, queue_size=1) #OLD message format


    for i in range(100): rate.sleep() #sleep for a while before taking readings
    

    while not rospy.is_shutdown():
        time_before = rospy.get_time()
        #print('in while loop')
        if balance_check == 1:
            #rospy.loginfo('Wrote to csv!!')
            curr_time = rospy.get_time()
            balance_frame.iloc[0, 0] = copy.deepcopy(curr_time)  
            seat_frame.iloc[0, 0] = copy.deepcopy(curr_time) 
            xsens_frame.iloc[0, 0] = copy.deepcopy(curr_time)
            #balance_frame.to_csv(fr'~/Documents/{part_input}_{trial_input}_Balance.csv', mode='a', header=False, index=False)  
            #seat_frame.to_csv(fr'~/Documents/{part_input}_{trial_input}_Seat.csv', mode='a', header=False, index=False)   
            #xsens_frame.to_csv(fr'~/Documents/{part_input}_{trial_input}_Xsens.csv', mode='a', header=False, index=False) 
            csv.write_csv(seat_frame, fr'~/Documents/{part_input}_{trial_input}_Seat.csv', mode='a', header=False, index=False) #pyarrow test
            csv.write_csv(balance_frame, fr'~/Documents/{part_input}_{trial_input}_Balance.csv', mode='a', header=False, index=False) #pyarrow test
            csv.write_csv(xsens_frame, fr'~/Documents/{part_input}_{trial_input}_Xsens.csv', mode='a', header=False, index=False) #pyarrow test
        print('time diff', rospy.get_time() - time_before)
        rate.sleep()

    #rospy.spin()


# def shutdown_csv_cleanup(): ## adds correct header stamps to slower sensor files
#     seat_csv = pd.read_csv(r'~/Documents/Seat_csv.csv')
#     balance_csv = pd.read_csv(r'~/Documents/Balance_csv.csv')
#     seat_csv['Header'] = balance_csv['Header']



if __name__ == '__main__':
    global part_input
    global trial_input
    global balance_check
    try:
        #inputting details to print on csvs
        part_input = input('Enter the participant code (3 capital letters): ')
        trial_input = input('Enter trial number (2 digits): ')
        #rospy.on_shutdown(shutdown_csv_cleanup)
        read_sensors()
    except rospy.ROSInterruptException:
        pass
