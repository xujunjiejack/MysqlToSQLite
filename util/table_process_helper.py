from collections import namedtuple, UserList
from enum import Enum

class ResultState(Enum):
    SUCCESS = 1
    FAILURE = 2
    WAITING = 3

class TableProcessResult:

    def __init__(self, table_name, result):
        self.table_name = table_name
        self.result = result

class TableProcessQueue:
    # This process queue will store the process procedure of a list table
    # You can apply different function in a chain. This queue will only
    # apply the later function onto the tables that get processed successfully last time

    def __init__(self, table_name_lists):
        self.process_list = [TableProcessResult(table, ResultState.WAITING) for table in table_name_lists]
        self.table_names = table_name_lists

    def success_tables(self):
        return self._list_of_tables_with_result_state_(ResultState.SUCCESS)

    def failure_tables(self):
        return self._list_of_tables_with_result_state_(ResultState.FAILURE)

    def _list_of_tables_with_result_state_(self, result_state):
        return [process_result.table_name for process_result in self.process_list
                if process_result.result == result_state]

    # This function should be type of table_name => bool
    # f is for each table inside
    def process_by(self,f):
        self._process_by_for_tables_with_filter_(f,
                            state_filter=lambda state: state == ResultState.SUCCESS or state == ResultState.WAITING)
        return self

    def process_all_tables_by(self, f):
        self._process_by_for_tables_with_filter_(f, lambda state: True)
        return self

    def process_by_functions_chain_(self, f_list):
        for table_result in self.process_list:
            for f in f_list:
                if table_result.result == ResultState.FAILURE:
                    break
                table_result.result = self._apply_function_to_table_(f, table_result.table_name)
        return self

    # This function should be type of table_name => bool
    # f is for each table inside
    def _process_by_for_tables_with_filter_(self, f, state_filter):
        # We only process the result that
        for table_result in self.process_list:
            if state_filter(table_result.result):
                # Run function
                table_result.result = self._apply_function_to_table_(f, table_result.table_name)

    def _apply_function_to_table_(self, f, table_name):
        function_success = f(table_name)
        if function_success:
            return ResultState.SUCCESS
        else:
            return ResultState.FAILURE
