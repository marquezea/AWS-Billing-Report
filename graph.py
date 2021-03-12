from termgraph import termgraph as tg

labels = ['2007', '2008', '2009',]
data = [[4], [14], [3]]
len_categories = 2
args = {'title': 'SERVICOS', 'width': 50,
        'format': '{:<5.2f}', 'suffix': '', 'no_labels': True, 'label_before': False,
        'color': None, 'vertical': False, 'histogram': False, 'stacked': False,
        'different_scale': False, 'calendar': False, 'no_values': False,
        'start_dt': None, 'custom_tick': '#', 'delim': '-', 'width': 20,
        'verbose': True, 'version': True}
colors = [91, 94]
tg.chart(labels, data, args, colors)
