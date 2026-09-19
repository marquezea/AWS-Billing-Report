from termgraph import termgraph as tg

labels = ['2007', '2008', '2009',]
data = [[4], [14], [3]]
len_categories = 2
args = {'title': 'SERVICOS', 
        'width': 100,
        'format': '{:<5.2f}', 
        'suffix': '', 
        'no_labels': False, 
        'label_before': False,
        'color': None, 
        'vertical': False, 
        'histogram': False, 
        'stacked': False,
        'different_scale': False, 
        'calendar': False, 
        'no_values': False,
        'start_dt': None, 
        'custom_tick': '💰', 
        'delim': '-', 
        'verbose': False, 
        'version': False}
colors = [91, 94]
data_obj = tg.Data(data=data, labels=labels)
tg.chart(data_obj, args, colors)
