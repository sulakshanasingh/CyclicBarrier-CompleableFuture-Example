package com.data.async;

import com.data.async.model.Employee;
import com.opencsv.bean.CsvToBeanBuilder;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@SpringBootApplication
public class AsynchronousApplication implements CommandLineRunner {
	private static final Map<String, Double> departmentAverages = new ConcurrentHashMap<>();

	public static void main(String[] args) {
		SpringApplication.run(AsynchronousApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		String csvPath ="C:\\Users\\project\\asynchronous\\src\\main\\resources\\employee.csv";
		//STEP1: Load employee dataset
		List<Employee> employeeList = new CsvToBeanBuilder<Employee>(new FileReader(csvPath))
				.withType(Employee.class).build().parse();
		//STEP2: Group employees by department
		Map<String, List<Employee>> departmentEmployees = employeeList.stream()
				.collect(Collectors.groupingBy(e -> e.getDept()));
		//STEP3: Configure CyclicBarrier to ensure synchronization between threads
		CyclicBarrier barrier = new CyclicBarrier(departmentEmployees.size(), () -> {
			System.out.println("All departments processed. Average salaries:");
			departmentAverages.forEach((dept, avg) ->
					System.out.printf("Department: %s, Average Salary: %.2f%n", dept, avg));
		});
		//STEP4: Executor for asynchronous processing
		ExecutorService executor = Executors.newCachedThreadPool();
		//STEP5: Process each department asynchronously
		List<CompletableFuture<Void>> futures = departmentEmployees.entrySet().stream()
				.map(entry -> CompletableFuture.runAsync(() -> {
					calculateAndStoreAverage(entry.getKey(), entry.getValue());
					try {
						// Wait for all departments to finish processing
						barrier.await();
					} catch (Exception e) {
						Thread.currentThread().interrupt();
						System.err.println("Barrier interrupted for department: " + entry.getKey());
					}
				}, executor))
				.collect(Collectors.toList());
		//STEP6: Wait for all tasks to complete
		CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
		executor.shutdown();
	}
	private static void calculateAndStoreAverage(String department, List<Employee> employees) {
		double average = employees.stream()
				.mapToDouble(emp -> emp.getSalary())
				.average()
				.orElse(0.0);
		departmentAverages.put(department, average);
	}
}
