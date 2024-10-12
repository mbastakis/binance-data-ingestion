import threading
from orchestrator.orchestrator import Orchestrator

def main():
    orchestrator = Orchestrator()
    stop_event = threading.Event()
    try:
        orchestrator.start()
        stop_event.wait()
    except KeyboardInterrupt:
        print("Stopping orchestrator...")
        orchestrator.stop()
    except Exception as e:
        print(f"An error occurred: {e}")
        orchestrator.stop()

if __name__ == '__main__':
    main()
