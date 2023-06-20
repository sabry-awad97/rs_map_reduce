use std::{fs::File, io::Read, path::PathBuf};

trait GenericInputData {
    fn read(&self) -> String;
}

trait Worker {
    fn map(&mut self);
    fn reduce(&mut self, other: &dyn Worker);
    fn get_result(&self) -> usize;
}

struct LineCountWorker {
    input_data: Box<dyn GenericInputData>,
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

impl GenericInputData for FileInputData {
    fn read(&self) -> String {
        let mut file = File::open(&self.file_path).expect("Failed to open file");
        let mut content = String::new();
        file.read_to_string(&mut content)
            .expect("Failed to read file");
        content
    }
}

fn main() {
    let input_data = Box::new(FileInputData {
        file_path: PathBuf::from("test_inputs/1"),
    });

    let mut worker = LineCountWorker {
        input_data,
        result: 0,
    };

    worker.map();

    println!("Lines: {}", worker.get_result());
}
