# Python program to explain os.mkdir() method
# importing os module
import os
import shutil
from dirsync import sync


# Directory
# with open('readme.txt', 'w') as f:
#     f.write('Create a new text file!')

def broker1(topic,value):

    if not os.path.isdir(rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage1\{topic}"):

        parent_dir = r"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage1"
        # Path
        path = os.path.join(parent_dir, topic)

        os.mkdir(path)


    # if not os.path.isdir(rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage1\{topic}"):
    #
    #     parent_dir = r"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage1"
    #     # Path
    #     path = os.path.join(parent_dir, topic)
    #
    #     os.mkdir(path)
    path=rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage1\{topic}"
    val=len(os.listdir(path))
    partition(val,topic,value,'storage1')



    source_path = rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage1"
    target_path1 = rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage2"
    sync(source_path, target_path1, 'sync')  # for syncing one way
    target_path2 = rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage3"
    sync(source_path, target_path2, 'sync')  # for syncing one way


def broker2(topic, value):
    if not os.path.isdir(rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage2"):
        parent_dir = r"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project"
        # Path
        path = os.path.join(parent_dir, storage1)
        os.mkdir(path)
    if not os.path.isdir(rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage2"):
        parent_dir = r"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project"
        # Path
        path = os.path.join(parent_dir, storage1)
        os.mkdir(path)
    if not os.path.isdir(rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage2\{topic}"):
        parent_dir = r"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage2"
        # Path
        path = os.path.join(parent_dir, topic)

        os.mkdir(path)

    # if not os.path.isdir(rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage1\{topic}"):
    #
    #     parent_dir = r"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage1"
    #     # Path
    #     path = os.path.join(parent_dir, topic)
    #
    #     os.mkdir(path)
    path = rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage2\{topic}"
    val = len(os.listdir(path))
    partition(val, topic, value,'storage2')

    source_path = rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage2"
    target_path1 = rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage3"
    sync(source_path, target_path1, 'sync')  # for syncing one way
    # target_path2 = rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\storage3"
    # sync(source_path, target_path2, 'sync')  # for syncing one way

def partition(val,topic,value,storage):
    if val==0:
        path = rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\{storage}\{topic}\{topic}_partition{val+1}.txt"
        with open(path, 'w') as f:
            f.write(f'{value}\n')
    else:
        with open(rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\{storage}\{topic}\{topic}_partition{val}.txt", 'r') as fp:
            x = len(fp.readlines())
        if x<5:
            with open(rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\{storage}\{topic}\{topic}_partition{val}.txt", 'a') as f:
                 f.write(f'{value}\n')
        else:
            path = rf"C:\Users\anshu\OneDrive\Desktop\sem5\BD\project\{storage}\{topic}\{topic}_partition{val + 1}.txt"
            with open(path, 'w') as f:
                f.write(f'{value}\n')

