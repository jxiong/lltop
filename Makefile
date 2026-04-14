all: lltop

lltop: lltop.py
	python3 -m PyInstaller --onefile lltop.py
	cp dist/lltop lltop
	chmod +x lltop

clean:
	rm -f lltop
	rm -rf build dist lltop.spec
