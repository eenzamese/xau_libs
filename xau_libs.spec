# -*- mode: python -*-

added_data = [('QuikPy\\QuikPy.py', 'QuikPy\\QuikPy.py')]


a = Analysis(['xau_libs.py'],
             binaries=[],
             datas=added_data,
             hiddenimports=[],
             hookspath=[],
             runtime_hooks=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data)
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    name='xau_libs',
    debug=False,
    strip=False,
    upx=True,
    console=True
)
