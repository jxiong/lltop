all: lltop llostio

lltop: lltop.py util.py
	python3 -m PyInstaller --onefile lltop.py
	cp dist/lltop lltop
	chmod +x lltop

llostio: llostio.py util.py
	python3 -m PyInstaller --onefile llostio.py
	cp dist/llostio llostio
	chmod +x llostio

clean:
	rm -f lltop llostio
	rm -rf build dist lltop.spec llostio.spec
