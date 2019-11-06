#!/usr/bin/env python

import re

def colorize(msg, background=None):
    
    # Generic styles ---
    msg = re.sub(r'(<p:)(.*?)(>)', r'<i><s>\2</i></s>', msg)          # path
    msg = re.sub(r'(<var:)(.*?)(>)', r'<b><i>\2</i></b>', msg)        # variable
    msg = re.sub(r'(<cls:)(.*?)(>)', r'<w>\2</w>', msg)               # python class
    msg = re.sub(r'(<h3:)(.*?)(>)', r'<s><y>\2</y></s>', msg)         # minor step
    msg = re.sub(r'(<h2:)(.*?)(>)', r'<m>\2</m>', msg)                # intermediate step
    msg = re.sub(r'(<v:)(.*?)(>)', r'<r>\2</r>', msg)                 # value
    msg = re.sub(r'(<h1:)(.*?)(>)', r'<m><s><u>\2</s></u></i>', msg)  # major step
    #msg = re.sub(r'(<hl:)(.*?)(>)', r'<s>\x1b[0;31;1;47m\2</bg></s></r>', msg)  # highlight
    msg = re.sub(r'(<w:)(.*?)(>)', r'<w>\2</w>', msg)

    # Hard coded styles ...
    if background is not None :
        bgd = '<%s>'%background
        for col in ['k', 'r', 'g', 'y', 'b', 'm', 'c', 'w']:
            msg = msg.replace('</%s>'%col, bgd)
        msg = bgd+msg+'\x1b[0m'
    # grey :
    msg = msg.replace('<k>', '\x1b[0;30m')
    msg = msg.replace('</k>', '\x1b[0m')
    # red :
    msg = msg.replace('<r>', '\x1b[0;31m')
    msg = msg.replace('</r>', '\x1b[0m')
    msg = msg.replace('<rbg>', '\x1b[0;41m')
    # Green
    msg = msg.replace('<g>', '\x1b[0;32m')
    msg = msg.replace('</g>', '\x1b[0m')
    # Yellow
    msg = msg.replace('<y>', '\x1b[0;33m')
    msg = msg.replace('</y>', '\x1b[0m')
    msg = msg.replace('<ybg>', '\x1b[0;43m')
    # Blue
    msg = msg.replace('<b>', '\x1b[0;34m')
    msg = msg.replace('</b>', '\x1b[0m')
    # Magenta
    msg = msg.replace('<m>', '\x1b[0;35m')
    msg = msg.replace('</m>', '\x1b[0m')
    # Cyan
    msg = msg.replace('<c>', '\x1b[0;36m')
    msg = msg.replace('</c>', '\x1b[0m')
    # White
    msg = msg.replace('<w>', '\x1b[0;37m')
    msg = msg.replace('</w>', '\x1b[0m')
    # Bold
    msg = msg.replace('<s>', '\x1b[1m')
    msg = msg.replace('</s>', '\x1b[22m')
    # Italic
    msg = msg.replace('<i>', '\x1b[3m')
    msg = msg.replace('</i>', '\x1b[23m')
    # Underlined
    msg = msg.replace('<u>', '\x1b[4m')
    msg = msg.replace('</u>', '\x1b[24m')

    # reset background
    msg = msg.replace('</bg>', '\x1b[0m')
    
    return msg
