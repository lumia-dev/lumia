import tkinter as tk
#import tkinter.ttk as ttk
#import sys
if(1>0):
    if (1>0):
        root = tk.Tk()
        root.grid_rowconfigure(0, weight=1)
        root.columnconfigure(0, weight=1)
        
        rootFrame = tk.Frame(root, bg="gray")
        rootFrame.grid(sticky='news')
        
        label1 = tk.Label(rootFrame, text="Label 1", fg="green")
        label1.grid(row=0, column=0, pady=(5, 0), sticky='nw')
        
        label2 = tk.Label(rootFrame, text="Label 2", fg="blue")
        label2.grid(row=1, column=0, pady=(5, 0), sticky='nw')
        
        label3 = tk.Label(rootFrame, text="Label 3", fg="red")
        label3.grid(row=3, column=0, pady=5, sticky='nw')
        
        # Create a frame for the canvas with non-zero row&column weights
        rootFrameCanvas = tk.Frame(rootFrame)
        rootFrameCanvas.grid(row=2, column=0, pady=(5, 0), sticky='nw')
        rootFrameCanvas.grid_rowconfigure(0, weight=1)
        rootFrameCanvas.grid_columnconfigure(0, weight=1)
        # Set grid_propagate to False to allow 5-by-5 buttons resizing later
        rootFrameCanvas.grid_propagate(False)
        
        # Add a scrollableCanvas in that frame
        scrollableCanvas = tk.Canvas(rootFrameCanvas, bg="yellow")
        scrollableCanvas.grid(row=0, column=0, sticky="news")
        
        # Link a scrollbar to the scrollableCanvas
        vsb = tk.Scrollbar(rootFrameCanvas, orient="vertical", command=scrollableCanvas.yview)
        vsb.grid(row=0, column=1, sticky='ns')
        scrollableCanvas.configure(yscrollcommand=vsb.set)
        
        # Create a frame to contain the buttons
        scrollableFrame4Widgets = tk.Frame(scrollableCanvas, bg="blue")
        scrollableCanvas.create_window((0, 0), window=scrollableFrame4Widgets, anchor='nw')
        
        # Add 9-by-5 buttons to the frame
        rows = 9
        columns = 5
        buttons = [[tk.Button() for j in range(columns)] for i in range(rows)]
        for i in range(0, rows):
            for j in range(0, columns):
                buttons[i][j] = tk.Button(scrollableFrame4Widgets, text=("%d,%d" % (i+1, j+1)))
                buttons[i][j].grid(row=i, column=j, sticky='news')
        
        # Update buttons frames idle tasks to let tkinter calculate buttons sizes
        scrollableFrame4Widgets.update_idletasks()
        
        # Resize the canvas frame to show exactly 5-by-5 buttons and the scrollbar
        first5columns_width = sum([buttons[0][j].winfo_width() for j in range(0, 5)])
        first5rows_height = sum([buttons[i][0].winfo_height() for i in range(0, 5)])
        rootFrameCanvas.config(width=first5columns_width + vsb.winfo_width(),
                            height=first5rows_height)
        
        # Set the scrollableCanvas scrolling region
        scrollableCanvas.config(scrollregion=scrollableCanvas.bbox("all"))
        
        # Launch the GUI
        root.mainloop()
