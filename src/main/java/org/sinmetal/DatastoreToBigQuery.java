package org.sinmetal;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.datastore.DatastoreIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Filter;
import com.google.datastore.v1.KindExpression;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.PropertyFilter.Operator;
import com.google.datastore.v1.PropertyReference;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.Value;
import com.google.protobuf.Timestamp;

public class DatastoreToBigQuery {

	static class EntityToTableRowFn extends DoFn<Entity, TableRow> {
		@Override
		public void processElement(ProcessContext c) {

			TableRow row = new TableRow();
			row.set("__key__", c.element().getKey().toString());
			for (Entry<String, Value> entry : c.element().getProperties().entrySet()) {
				System.out.println("irontiger:" + entry.getKey() + ":" + entry.getValue().toString());

				switch (entry.getValue().getValueTypeCase()) {
				case STRING_VALUE:
					row.set(entry.getKey(), entry.getValue().getStringValue());
					break;
				case INTEGER_VALUE:
					row.set(entry.getKey(), entry.getValue().getIntegerValue());
					break;
				case DOUBLE_VALUE:
					row.set(entry.getKey(), entry.getValue().getDoubleValue());
					break;
				case TIMESTAMP_VALUE:
					row.set(entry.getKey(), entry.getValue().getTimestampValue().getSeconds());
					break;
				default:
					System.out.println(
							"irontiger:UnknownValueType:" + entry.getKey() + ":" + entry.getValue().toString());
					break;
				}
			}
			c.output(row);
		}
	}

	public static class EntityToTableRow extends PTransform<PCollection<Entity>, PCollection<TableRow>> {
		@Override
		public PCollection<TableRow> apply(PCollection<Entity> entities) {

			PCollection<TableRow> rows = entities.apply(ParDo.of(new EntityToTableRowFn()));

			return rows;
		}
	}

	public interface DatastoreToBigQueryOptions extends DataflowPipelineOptions {
	}

	public static void main(String[] args) {
		DatastoreToBigQueryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(DatastoreToBigQueryOptions.class);
		Pipeline p = Pipeline.create(options);

		Filter filter = Filter.newBuilder()
				.setPropertyFilter(
						PropertyFilter.newBuilder().setOp(Operator.GREATER_THAN)
								.setValue(Value.newBuilder()
										.setTimestampValue(Timestamp.newBuilder()
												.setSeconds(new Date(1483196400000L).getTime() / 1000)))
								.setProperty(PropertyReference.newBuilder().setName("CreatedAt").build()).build())
				.build();
		Query query = Query.newBuilder().addKind(KindExpression.newBuilder().setName("Ticket").build())
				.setFilter(filter).build();

		PCollection<Entity> entities = p
				.apply(DatastoreIO.v1().read().withProjectId(options.getProject()).withQuery(query));
		PCollection<TableRow> rows = entities.apply(new EntityToTableRow());

		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("__key__").setType("STRING"));
		fields.add(new TableFieldSchema().setName("ClassType").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("CreatedAt").setType("TIMESTAMP"));
		fields.add(new TableFieldSchema().setName("ScheduleKey").setType("STRING"));
		fields.add(new TableFieldSchema().setName("TrainingCourseType").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("UpdatedAt").setType("TIMESTAMP"));
		fields.add(new TableFieldSchema().setName("UserID").setType("STRING"));

		TableSchema schema = new TableSchema().setFields(fields);

		rows.apply(BigQueryIO.Write.named("Write").to("training-topgate-dev:output.output_table").withSchema(schema)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

		p.run();
	}

}
