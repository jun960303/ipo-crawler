# main.py
import tkinter as tk
from gui.app import IPOApp

if __name__ == "__main__":
    root = tk.Tk()
    app = IPOApp(root)
    root.mainloop()
    print("정상종료")
