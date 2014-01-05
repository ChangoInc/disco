from disco.test import TestCase, TestJob
from disco.core import Job
import disco
import time
import threading

def map(line, params):
    for word in line.split():
        yield word, 1

def reduce(iter, params):
    from disco.util import kvgroup
    for word, counts in kvgroup(sorted(iter)):
        yield word, sum(counts)

PORT = 1234

def startServer():
    import subprocess
    serverline="HTTP/1.1 200 OK\nContent-Type: text/plain\
            \nTransfer-Encoding: chunked\n\nb\nHello World\n0\n"
    p1 = subprocess.Popen(["echo", serverline], stdout=subprocess.PIPE)
    p2 = subprocess.Popen(["nc", "-l", "-p", str(PORT), "-C"], stdin=p1.stdout, stdout=subprocess.PIPE)
    p1.stdout.close()
    output,err = p2.communicate()

class RawTestCase(TestCase):
    def runTest(self):
        threading.Thread(target=startServer).start()
        input = 'http://localhost:' + str(PORT)
        self.job = Job().run(input=[input], map=map, reduce=reduce)
        self.assertEqual(sorted(self.results(self.job)), [('Hello', 1), ('World', 1)])
