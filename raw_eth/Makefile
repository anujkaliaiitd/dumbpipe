all:
	g++ -O3 -std=c++11 -g -o receiver receiver.cc -libverbs -lpthread
	g++ -O3 -std=c++11 -g -o sender sender.cc -libverbs
clean:
	rm sender receiver
