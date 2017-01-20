#undef NDEBUG
#include <iostream>
#include <cassert>
#include <vector>
#include "Transaction.hh"
#include "TBox.hh"
#include "LogApply.hh"

int main() {
  TBox<int> f;
  Transaction::register_object(f, 0);
  LogApply::listen(1, 2000);
}
