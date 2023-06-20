use std::{
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

trait GenericInputData {
    fn read(&self) -> String;
}

trait Worker {
    fn map(&mut self);
    fn reduce(&mut self, other: &dyn Worker);
    fn get_result(&self) -> usize;
}

#[derive(Clone)]
struct LineCountWorker {
    input_data: Arc<Box<dyn GenericInputData>>,
    result: usize,
}

impl Worker for LineCountWorker {
    fn map(&mut self) {
        let data = self.input_data.read();
        self.result = data.lines().count();
    }

    fn reduce(&mut self, other: &dyn Worker) {
        self.result += other.get_result();
    }

    fn get_result(&self) -> usize {
        self.result
    }
}

struct FileInputData {
    file_path: PathBuf,
}

impl FileInputData {
    fn new(file_path: PathBuf) -> Self {
        Self { file_path }
    }
}

impl GenericInputData for FileInputData {
    fn read(&self) -> String {
        let mut file = File::open(&self.file_path).expect("Failed to open file");
        let mut content = String::new();
        file.read_to_string(&mut content)
            .expect("Failed to read file");
        content
    }
}

fn generate_inputs(data_dir: &str) -> Vec<Box<dyn GenericInputData>> {
    let path = Path::new(data_dir);
    let mut inputs = Vec::new();
    if path.is_dir() {
        for entry in fs::read_dir(path)
            .expect("Failed to read directory")
            .flatten()
        {
            let file_path = entry.path();
            let input_data = Box::new(FileInputData::new(file_path));
            inputs.push(input_data as Box<dyn GenericInputData>);
        }
    }
    inputs
}

fn create_workers(input_list: Vec<Box<dyn GenericInputData>>) -> Vec<Arc<Mutex<dyn Worker>>> {
    let mut workers = Vec::new();

    for input_data in input_list {
        let worker = Arc::new(Mutex::new(LineCountWorker {
            input_data: input_data.into(),
            result: 0,
        }));

        workers.push(worker as Arc<Mutex<dyn Worker>>);
    }

    workers
}

fn main() {
    let input_list = generate_inputs("test_inputs");
    let mut workers = create_workers(input_list);

    if let Some(first_worker) = workers.first().cloned() {
        let mut first_worker = first_worker.lock().unwrap();
        
        for worker in workers.iter_mut().skip(1) {
            let mut worker = worker.lock().unwrap();
            worker.map();
            first_worker.reduce(&*worker);
        }

        println!("Lines: {}", first_worker.get_result());
    }
}
