use std::{
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

trait GenericInputData<T> {
    fn read(&self) -> T;
}

trait Mapper {
    fn map(&mut self);
}

trait Reducer {
    fn reduce(&mut self, other: &dyn MapReducer);
    fn get_result(&self) -> usize;
}

trait MapReducer: Mapper + Reducer {}

#[derive(Clone)]
struct LineCountWorker {
    input_data: Arc<dyn GenericInputData<String>>,
    result: usize,
}

impl Mapper for LineCountWorker {
    fn map(&mut self) {
        let data = self.input_data.read();
        self.result = data.lines().count();
    }
}

impl Reducer for LineCountWorker {
    fn reduce(&mut self, other: &dyn MapReducer) {
        self.result += other.get_result();
    }

    fn get_result(&self) -> usize {
        self.result
    }
}

impl MapReducer for LineCountWorker {}

struct FileInputData {
    file_path: PathBuf,
}

impl FileInputData {
    fn new(file_path: PathBuf) -> Self {
        Self { file_path }
    }
}

impl<T> GenericInputData<T> for FileInputData
where
    T: From<String>,
{
    fn read(&self) -> T {
        let mut file = File::open(&self.file_path).expect("Failed to open file");
        let mut content = String::new();
        file.read_to_string(&mut content)
            .expect("Failed to read file");
        T::from(content)
    }
}

fn generate_inputs<T>(data_dir: &str) -> Vec<Box<dyn GenericInputData<T>>>
where
    T: From<String>,
{
    let path = Path::new(data_dir);
    let mut inputs = Vec::new();
    if path.is_dir() {
        for entry in fs::read_dir(path)
            .expect("Failed to read directory")
            .flatten()
        {
            let file_path = entry.path();
            let input_data =
                Box::new(FileInputData::new(file_path)) as Box<dyn GenericInputData<T>>;
            inputs.push(input_data);
        }
    }
    inputs
}

fn create_workers(
    input_list: Vec<Box<dyn GenericInputData<String>>>,
) -> Vec<Arc<Mutex<dyn MapReducer>>> {
    let mut workers = Vec::new();

    for input_data in input_list {
        let worker = Arc::new(Mutex::new(LineCountWorker {
            input_data: input_data.into(),
            result: 0,
        }));

        workers.push(worker as Arc<Mutex<dyn MapReducer>>);
    }

    workers
}

fn main() {
    let input_list = generate_inputs::<String>("test_inputs");
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
