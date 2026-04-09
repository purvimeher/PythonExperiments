colors = ['red', 'orange', 'yellow', 'green', 'blue', 'indigo', 'violet']

sub_colors = colors[0:4]

print(sub_colors)


sub_colors = colors[:3]

print(sub_colors)


sub_colors = colors[-3:]

print(sub_colors)

sub_colors = colors[::2]

print(sub_colors)

del colors[2:5]

print(colors)