all:
	g++ -std=c++11 -g -o receiver receiver.cc -libverbs -lpthread
	g++ -std=c++11 -g -o sender sender.cc -libverbs
clean:
	rm sender receiver
