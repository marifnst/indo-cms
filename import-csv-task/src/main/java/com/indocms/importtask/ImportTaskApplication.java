package com.indocms.importtask;

import com.indocms.importtask.service.ImportService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.cloud.task.listener.annotation.AfterTask;
import org.springframework.cloud.task.listener.annotation.BeforeTask;
import org.springframework.cloud.task.listener.annotation.FailedTask;
import org.springframework.cloud.task.repository.TaskExecution;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableTask
public class ImportTaskApplication {

	@Bean
	public ApplicationRunner commandLineRunner() {
		return new ImportApplicationRunner();
	}

	public static class ImportApplicationRunner implements ApplicationRunner {

		@Autowired
		ImportService importService;

		@BeforeTask
		public void beforeTask(TaskExecution taskExecution) {
			System.out.println("before task");
		}

		@Override
		public void run(ApplicationArguments args) throws Exception {
			importService.process(args);
		}

		@AfterTask
		public void afterTask(TaskExecution taskExecution) {
			System.out.println("after task");
		}

		@FailedTask
		public void failedTask(TaskExecution taskExecution, Throwable throwable) {
			System.out.println("failed task");
		}
	}

	public static void main(String[] args) {
		SpringApplication.run(ImportTaskApplication.class, args);
	}
}
