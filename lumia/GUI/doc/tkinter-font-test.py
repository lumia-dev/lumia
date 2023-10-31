import tkinter as tk
import tkinter.font as tkFont
from tkinter import ttk
import os

if os.environ.get('DISPLAY','') == '':
    print('no display found. Using :0.0')
    os.environ.__setitem__('DISPLAY', ':0.0')

root = tk.Tk()

root.geometry("510x310")
# Create A Main frame and then add scrollbars
# Scrollbar ideas taken from 4th reply (jeromba6) to: https://stackoverflow.com/questions/19860047/python-tkinter-scrollbar-for-entire-window
main_frame = tk.Frame(root)
main_frame.pack(fill=tk.BOTH,expand=1)

# Create Frame for X Scrollbar
sec = tk.Frame(main_frame)
sec.pack(fill=tk.X,side=tk.BOTTOM)

# Create a Canvas
myCanvas = tk.Canvas(main_frame) #, width=500, height=300, yscrollcommand = scrollbar.set)
myCanvas.pack(side=tk.LEFT,fill=tk.BOTH,expand=1)

# Add A Scrollbars to Canvas
x_scrollbar = ttk.Scrollbar(sec,orient=tk.HORIZONTAL,command=myCanvas.xview)
x_scrollbar.pack(side=tk.BOTTOM,fill=tk.X)
y_scrollbar = ttk.Scrollbar(main_frame,orient=tk.VERTICAL,command=myCanvas.yview)
y_scrollbar.pack(side=tk.RIGHT,fill=tk.Y)


# Configure the canvas
myCanvas.configure(xscrollcommand=x_scrollbar.set)
myCanvas.configure(yscrollcommand=y_scrollbar.set)
myCanvas.bind("<Configure>",lambda e: myCanvas.config(scrollregion= myCanvas.bbox(tk.ALL))) 

# Create Another Frame INSIDE the Canvas
activeFrame = tk.Frame(myCanvas)

# Add that New Frame a Window In The Canvas
myCanvas.create_window((0,0),window= activeFrame, anchor="nw")

#for thing in range(100):
#    tk.Button(activeFrame ,text=f"Button  {thing}").grid(row=5,column=thing,pady=10,padx=10)

#for thing in range(100):
#    tk.Button(activeFrame ,text=f"Button  {thing}").grid(row=thing,column=5,pady=10,padx=10)

#root.mainloop()


(x,y) = (5,5)
fonts = []
print ("specifying font sizes in pts via the system:")
for (family,size) in [("times",12),("times",14),("times",15),("times",16),("times",24),("Georgia",10),("Georgia",11),("Georgia",12),("Georgia",13),("Georgia",14),("Georgia",15),("Georgia",16),("Georgia",17),("Georgia",20),("Georgia",24)]:
    text = f"yellow world / {family} {size}"
    font = tkFont.Font(family=family, size=size)
    (w,h) = (font.measure(text),font.metrics("linespace"))
    print ("%s %s: (%s,%s),   Actual font used: %s" % (family,size,w,h,font.actual()))
    myCanvas.create_rectangle(x,y,x+w,y+h)
    myCanvas.create_text(x,y,text=text,font=font,anchor=tk.NW)
    fonts.append(font) # save object from garbage collecting
    y += h+5

root.mainloop()
# tk.mainloop()
