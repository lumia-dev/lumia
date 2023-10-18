import tkinter as tk
import tkinter.font as tkFont
root = tk.Tk()
canvas = tk.Canvas(root, width=300, height=800)
canvas.pack()
(x,y) = (5,5)
text = "yellow world"
fonts = []
print ("specifying font sizes in pts via the system:")
for (family,size) in [("times",12),("times",14),("times",15),("times",16),("times",24),("Georgia",10),("Georgia",11),("Georgia",12),("Georgia",13),("Georgia",14),("Georgia",15),("Georgia",16),("Georgia",17),("Georgia",20),("Georgia",24)]:
    font = tkFont.Font(family=family, size=size)
    (w,h) = (font.measure(text),font.metrics("linespace"))
    print ("%s %s: (%s,%s),   Actual font used: %s" % (family,size,w,h,font.actual()))
    canvas.create_rectangle(x,y,x+w,y+h)
    canvas.create_text(x,y,text=text,font=font,anchor=tk.NW)
    fonts.append(font) # save object from garbage collecting
    y += h+5

print ("specifying font sizes in pixels:")
for (family,size) in [("times",-12),("times",-14),("times",-15),("times",-16),("times",-24),("Georgia",-10),("Georgia",-11),("Georgia",-12),("Georgia",-13),("Georgia",-14),("Georgia",-15),("Georgia",-16),("Georgia",-17),("Georgia",-20),("Georgia",-24)]:
    font = tkFont.Font(family=family, size=size)
    (w,h) = (font.measure(text),font.metrics("linespace"))
    print ("%s %s: (%s,%s),   Actual font used: %s" % (family,size,w,h,font.actual()))
    canvas.create_rectangle(x,y,x+w,y+h)
    canvas.create_text(x,y,text=text,font=font,anchor=tk.NW)
    fonts.append(font) # save object from garbage collecting
    y += h+5

tk.mainloop()
