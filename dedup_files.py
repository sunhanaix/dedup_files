import os, sys, re, json, time
import hashlib
from collections import defaultdict
import ast
import util
import concurrent.futures
sys.stdout.reconfigure(encoding='utf-8')

SUPPORT_HARD_LINK_FS=['ext2','ext2','ext4','xfs','zfs','ntfs'] #fat和exfat文件系统，都不支持硬链接

#本程序，扫描给定目录及其子目录的所有文件，并计算每个文件的md5值（假定md5没有碰撞），结果存入md5_dict

app_path = os.path.dirname(os.path.abspath(sys.argv[0]))
log_file = os.path.basename(sys.argv[0]).split('.')[0] + '.log'
cfg_file=os.path.basename(sys.argv[0]).split('.')[0] + '.ini'

md5_dict=defaultdict(list)
ino_dict=defaultdict(list)
md5_array=[]

mylog=util.get_logger(log_file) #设置写入日志函数
cfg=util.get_cfg(cfg_file) #从配置文件中读入配置，如果文件不存在，则创建一个
mylog.info(json.dumps(cfg,indent=2,ensure_ascii=True))

#检查配置的去重目录是否支持硬链接，如果不支持，进行警告提醒，并直接退出
for dir in cfg['dirs']:
    fs_type=util.get_fs_type(dir)
    if fs_type in SUPPORT_HARD_LINK_FS:
        mylog.info(f"{dir}所在文件系统为{fs_type}，支持硬链接")
    else:
        print()
        mylog.error(f"{dir}所在文件系统为{fs_type}，不支持硬链接，请重新调整{cfg_file}中dirs的设置")
        print()
        input("按回车键退出")
        sys.exit(-1)

def write_cache_file(cache_data,fname=None): #把所有的内存中的每个文件的md5的数据，按一条条格式，全部dump到cache_file里面去
    if not fname:
        fname=cfg['cache_file']
    with open(fname, 'w', encoding='utf-8') as file:
        for entry in cache_data:
            # 使用repr()将字典转换为字符串，并写入文件
            file.write(repr(entry) + '\n')

def append_record_to_cache(record_data): #把当前这么一条数据，按照字典一行的模式，追加到cache_file中去
    # 使用'a'模式打开文件，以便追加数据
    with open(cfg['cache_file'], 'a', encoding='utf-8') as file:
        # 使用repr()将字典转换为字符串，并追加到文件末尾
        file.write(repr(record_data) + '\n')

def read_cache_file(): #把cache中所有文件的md5的数值，全部读入到内存中来，返回cache_data的数组
    cache_data = []
    if not os.path.isfile(cfg['cache_file']):
        return []
    with open(cfg['cache_file'], 'r', encoding='utf-8') as file:
        for line in file:
            # 使用ast.literal_eval将文本行解析为字典
            data = ast.literal_eval(line.strip())
            cache_data.append(data)
    return cache_data

def convert_array_to_dict(input_array): #把md5的{'file_path':file_path,'size':size,'md5':md5_value}数组，转换成基于每一个md5的hash dict中，dict的key为md5值
    result_dict =defaultdict(list)
    for item in input_array:
        if item['md5'] not in result_dict:
            result_dict[item['md5']] = [item]
        else:
            result_dict[item['md5']].append(item)
    return result_dict

def convert_array_to_ino_dict(input_array): #把md5的{'file_path':file_path,'size':size,'md5':md5_value}数组，转换成基于每一个ino的hash dict中，dict的key为ino值
    result_dict =defaultdict(list)
    for item in input_array:
        if item['ino'] not in result_dict:
            result_dict[item['ino']] = [item]
        else:
            result_dict[item['ino']].append(item)
    return result_dict

def convert_dict_to_array(input_dict): #把基于md5做hash key的dict，转换为md5的{'file_path':file_path,'size':size,'md5':md5_value}数组
    result = []
    for uuid in input_dict:
        result.append(input_dict[uuid])
    return result

def find_record_by_file_path(file_path):
    #给定一个file_path，检查是否已经在md5_array列表里面，如果在的话，返回True，否则False
    #如果没缓存过，或者没变化，会返回None
    global md5_array
    for record in md5_array:
        if file_path== record['file_path']:
            return True
    return None

def calculate_md5(file_path, md5_dict, ino_dict, md5_array):
    # 计算单个文件的MD5哈希值，以及获得文件属性信息
    try:
        md5_value = util.md5_file(file_path)
        size = os.stat(file_path).st_size
        ino = os.stat(file_path).st_ino
        item_record = {'file_path': file_path, 'size': size, 'md5': md5_value, 'ino': ino}
        if ino in ino_dict:
            return  # Skip hard links
        md5_dict[md5_value].append(item_record)
        ino_dict[ino].append(item_record)
        md5_array.append(item_record)
        append_record_to_cache(item_record)
    except Exception as e:
        mylog.error(f"Error calculating MD5 for {file_path}: {e}")


def get_md5_info():
    # 用于存储文件MD5哈希值和路径的字典
    global md5_array, md5_dict, ino_dict
    i = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=cfg['max_workers']) as executor:
        for directory in cfg['dirs']:
            mylog.info(f"遍历{directory}获得每一个文件中……")
            for root, _, files in os.walk(directory):
                for file in files:
                    file_path = os.path.join(root, file)
                    i += 1
                    ss_file_path = util.remove_unprintable_chars(file_path)
                    log_ss = f"{i=},{ss_file_path},"
                    if find_record_by_file_path(file_path):
                        mylog.info(f"{log_ss} in cache, will pass")
                        continue
                    mylog.info(f"{log_ss} not in cache, will calculate md5 now")
                    executor.submit(calculate_md5, file_path, md5_dict, ino_dict, md5_array)

    open(cfg['md5_key_file'], 'w', encoding='utf8').write(json.dumps(md5_dict, indent=2, ensure_ascii=False))
    return md5_dict


#从cache文件中，载入已经算过的md5数据
md5_array=read_cache_file()
md5_dict = convert_array_to_dict(md5_array)
ino_dict = convert_array_to_ino_dict(md5_array)

#开始计算每一个文件的md5
stime=time.time()
md5_dict = get_md5_info()
etime=time.time()
mylog.info(f"遍历所有目录完成，耗时{etime-stime}秒，现在比对重复文件")

#统计当前有哪些文件有多份副本，如果删除它们，可以节约多少空间
duplicate_records=[]
save_size=0 #可以节约的空间
to_del_md5_dict=defaultdict(list) #存储需要删除文件清单
for md5_value in md5_dict:
    records=md5_dict[md5_value]
    src_file=records[0]['file_path'] #取第一个记录作为源
    try:
        src_ino=os.stat(src_file).st_ino  #由于有cache缓存里记录了这个文件，就会用最开始的inode number，即使后面删除文件，做了硬链接，也没有更新cache.dat和md5_key_files.dat，因此这里现获得一遍
    except Exception as e:
        mylog.warning(f"{src_file}这个文件可能没有了,现在跳过这个文件，reason:{e}")
        continue
    if len(records) > 1: #只有副本数量>1的，才是重复文件
        for other_record in records[1:]:  #把副本的第一个记录去掉，剩下的几个记录，就都是可以节约空间的
            cur_file=other_record['file_path']
            try:
                cur_ino=os.stat(cur_file).st_ino
            except Exception as e:
                mylog.warning(f"{cur_file}这个文件可能没有了,现在跳过这个文件，reason:{e}")
                continue
            if src_ino==cur_ino: #如果两个文件的inode number相同，认为是一个硬链接，那么跳过这个
                continue
            if md5_value in to_del_md5_dict:
                to_del_md5_dict[md5_value].append(other_record)
            else:
                to_del_md5_dict[md5_value]=[records[0],other_record]
            save_size+=other_record['size']

cnt_to_del_couple_files=0
cnt_to_del_files=0

for md5_value in to_del_md5_dict:
    cnt_to_del_couple_files+=1
    cnt_to_del_files+=len(to_del_md5_dict[md5_value])

if save_size==0:
    mylog.info("当前没有重复副本需要优化，可优化空间为0字节")
    input("按回车键退出")
    sys.exit(0)

open(cfg['to_del_file'],'w',encoding='utf8').write(json.dumps(to_del_md5_dict,indent=2,ensure_ascii=False))

if cfg['ask_before_del']:
    a=input(f"共有{cnt_to_del_couple_files}文件组（共{cnt_to_del_files}个文件），预计可节约{save_size/1024/1024:0.2f}MB空间，是否继续？(Y/N)")
    if not a.lstrip().rstrip().lower()=='y':
        mylog.info("你没有选择Y，本程序将会退出，不进行删除副本操作")
        print()
        input("按回车键退出")
        sys.exit()

cnt_real_del_files=0 #实际删除文件数量
size_real_del_files=0 #实际删除掉的文件，释放的空间数量
# 创建硬链接
for md5_value in md5_dict:
    records=md5_dict[md5_value]
    if len(records) > 1:
        # 找到重复的文件，保留一个，删除其它的，然后创建硬链接
        reference_file = records[0]['file_path']
        for other_record in records[1:]:
            duplicate_file=other_record['file_path']
            try:
                mylog.warning(f"deleting {duplicate_file}")
            except  Exception as e:
                mylog.error(e)
                continue
            try:
                os.remove(duplicate_file)
                cnt_real_del_files += 1
                size_real_del_files += other_record['size']
            except Exception as e:
                mylog.error(f"remove file failed,reason:{e}")
                continue
            try:
                mylog.info(f"link from {reference_file} to {duplicate_file}")
            except Exception as e:
                mylog.error(e)
                continue
            try:
                os.link(reference_file, duplicate_file)
            except Exception as e:
                mylog.error(f"link file failed,reason:{e}")
                continue
mylog.info(f"共整理{cnt_real_del_files}个文件，释放了{size_real_del_files/1024/1024:0.2f}MB空间")
print()
input("按回车键退出")
